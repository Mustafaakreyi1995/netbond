package app.netbond

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.net.VpnService
import android.os.Build
import android.os.ParcelFileDescriptor
import app.netbond.core.mobile.Callbacks
import app.netbond.core.mobile.Mobile
import java.net.Inet4Address
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

/**
 * Whole-phone VPN. Every app's traffic enters the VPN interface, the Go core
 * turns it into streams and spreads them over one link per available network
 * (Wi-Fi, mobile data, Ethernet/USB) to the NetBond server.
 */
class BondVpnService : VpnService(), Callbacks {

    companion object {
        const val ACTION_START = "app.netbond.START"
        const val ACTION_STOP = "app.netbond.STOP"
        const val EXTRA_CODE = "code"
        private const val CHANNEL = "vpn"
        private const val NOTIF_ID = 1
        private const val MTU = 1500

        @Volatile
        var running = false

        private val logLines = ArrayDeque<String>()
        private val stamp = SimpleDateFormat("HH:mm:ss", Locale.US)

        fun addLog(msg: String) {
            synchronized(logLines) {
                logLines.addLast(stamp.format(Date()) + "  " + msg)
                while (logLines.size > 200) logLines.removeFirst()
            }
        }

        fun logText(): String = synchronized(logLines) { logLines.joinToString("\n") }
    }

    private data class Transport(val id: String, val label: String, val type: Int)

    private val transports = listOf(
        Transport("wifi", "Wi-Fi", NetworkCapabilities.TRANSPORT_WIFI),
        Transport("cell", "Mobile data", NetworkCapabilities.TRANSPORT_CELLULAR),
        Transport("eth", "Ethernet / USB", NetworkCapabilities.TRANSPORT_ETHERNET),
    )

    private val networks = ConcurrentHashMap<String, Network>()
    private val callbacks = ArrayList<ConnectivityManager.NetworkCallback>()
    private val worker = Executors.newSingleThreadExecutor()
    private var host = ""

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            shutdown()
            return START_NOT_STICKY
        }
        val code = intent?.getStringExtra(EXTRA_CODE)
            ?: getSharedPreferences("netbond", Context.MODE_PRIVATE).getString("code", "")
            ?: ""
        startForegroundNow()
        if (running) return START_STICKY
        try {
            startTunnel(code)
        } catch (e: Exception) {
            addLog("Could not start: " + (e.message ?: e.toString()))
            shutdown()
            return START_NOT_STICKY
        }
        return START_STICKY
    }

    private fun startTunnel(code: String) {
        host = Mobile.serverHost(code)
        if (host.isEmpty()) throw IllegalArgumentException("the connection code is not valid")

        val builder = Builder()
            .setSession("NetBond")
            .setMtu(MTU)
            .addAddress("10.111.0.2", 30)
            .addRoute("0.0.0.0", 0)
            .addAddress("fd00:6e62::2", 126)
            .addRoute("::", 0)
            .addDnsServer("1.1.1.1")
            .addDnsServer("8.8.8.8")
            // our own sockets to the server must not loop back into the VPN
            .addDisallowedApplication(packageName)
        if (Build.VERSION.SDK_INT >= 29) builder.setMetered(false)

        val pfd: ParcelFileDescriptor = builder.establish()
            ?: throw IllegalStateException("VPN permission was not granted")
        Mobile.start(pfd.detachFd(), MTU, code, this)
        running = true
        addLog("VPN started, server $host")

        val cm = getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        for (t in transports) {
            val req = NetworkRequest.Builder()
                .addTransportType(t.type)
                .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                .build()
            val cb = object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) {
                    networks[t.id] = network
                    addLog(t.label + ": network available")
                    worker.execute { attach(t, network) }
                }

                override fun onLost(network: Network) {
                    if (networks[t.id] == network) {
                        networks.remove(t.id)
                        addLog(t.label + ": network lost")
                        worker.execute { if (running) Mobile.removeLink(t.id) }
                    }
                }
            }
            try {
                // requestNetwork (not just a listener) is what keeps mobile
                // data up while Wi-Fi is connected.
                cm.requestNetwork(req, cb)
                callbacks.add(cb)
            } catch (e: Exception) {
                addLog(t.label + ": " + e.message)
            }
        }
    }

    /** Resolve the server on this particular network, then start the link. */
    private fun attach(t: Transport, network: Network) {
        if (!running || networks[t.id] != network) return
        try {
            val addrs = network.getAllByName(host)
            val addr = addrs.firstOrNull { it is Inet4Address } ?: addrs.first()
            Mobile.addLink(t.id, t.label, addr.hostAddress ?: host)
        } catch (e: Exception) {
            addLog(t.label + ": cannot resolve server (" + e.message + ")")
        }
    }

    // ---- callbacks from the Go core ----

    override fun bindSocket(linkID: String?, fd: Int): Boolean {
        val network = networks[linkID ?: return false] ?: return false
        return try {
            val pfd = ParcelFileDescriptor.fromFd(fd)
            try {
                network.bindSocket(pfd.fileDescriptor)
            } finally {
                pfd.close()
            }
            true
        } catch (e: Exception) {
            false
        }
    }

    override fun log(msg: String?) {
        if (msg != null) addLog(msg)
    }

    // ---- lifecycle ----

    override fun onRevoke() {
        addLog("VPN permission revoked")
        shutdown()
    }

    override fun onDestroy() {
        shutdown()
        super.onDestroy()
    }

    private fun shutdown() {
        val cm = getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        for (cb in callbacks) {
            try {
                cm.unregisterNetworkCallback(cb)
            } catch (_: Exception) {
            }
        }
        callbacks.clear()
        networks.clear()
        if (running) {
            running = false
            try {
                Mobile.stop()
            } catch (_: Exception) {
            }
            addLog("VPN stopped")
        }
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun startForegroundNow() {
        val nm = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL, "Connection status", NotificationManager.IMPORTANCE_LOW)
        )
        val open = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE
        )
        val stop = PendingIntent.getService(
            this, 1, Intent(this, BondVpnService::class.java).setAction(ACTION_STOP),
            PendingIntent.FLAG_IMMUTABLE
        )
        val n: Notification = Notification.Builder(this, CHANNEL)
            .setSmallIcon(R.drawable.ic_stat)
            .setContentTitle("NetBond is on")
            .setContentText("Combining your connections")
            .setContentIntent(open)
            .setOngoing(true)
            .addAction(Notification.Action.Builder(null, "Disconnect", stop).build())
            .build()
        if (Build.VERSION.SDK_INT >= 34) {
            startForeground(NOTIF_ID, n, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
        } else {
            startForeground(NOTIF_ID, n)
        }
    }
}
