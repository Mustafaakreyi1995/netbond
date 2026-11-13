package app.netbond

import android.Manifest
import android.app.Activity
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.VpnService
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.Gravity
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.Switch
import android.widget.TextView
import android.widget.Toast
import app.netbond.core.mobile.Mobile
import org.json.JSONObject
import java.util.Locale

class MainActivity : Activity() {

    private lateinit var status: TextView
    private lateinit var down: TextView
    private lateinit var up: TextView
    private lateinit var toggle: Button
    private lateinit var code: EditText
    private lateinit var links: LinearLayout
    private lateinit var log: TextView
    private lateinit var useServer: Switch
    private lateinit var serverBox: View
    private lateinit var modeHint: TextView

    private val handler = Handler(Looper.getMainLooper())
    private val prev = HashMap<String, LongArray>() // id -> [tx, rx]
    private var prevAt = 0L

    private val tick = object : Runnable {
        override fun run() {
            refresh()
            handler.postDelayed(this, 1000)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        status = findViewById(R.id.status)
        down = findViewById(R.id.down)
        up = findViewById(R.id.up)
        toggle = findViewById(R.id.toggle)
        code = findViewById(R.id.code)
        links = findViewById(R.id.links)
        log = findViewById(R.id.log)

        useServer = findViewById(R.id.useServer)
        serverBox = findViewById(R.id.serverBox)
        modeHint = findViewById(R.id.modeHint)

        val prefs = getSharedPreferences("netbond", Context.MODE_PRIVATE)
        code.setText(prefs.getString("code", ""))
        useServer.isChecked = prefs.getBoolean("useServer", false)
        showMode()
        useServer.setOnCheckedChangeListener { _, on ->
            prefs.edit().putBoolean("useServer", on).apply()
            showMode()
        }

        findViewById<Button>(R.id.paste).setOnClickListener {
            val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
            val clip = cm.primaryClip
            if (clip != null && clip.itemCount > 0) {
                code.setText(clip.getItemAt(0).coerceToText(this).toString().trim())
            }
        }

        toggle.setOnClickListener {
            if (BondVpnService.running) {
                startService(Intent(this, BondVpnService::class.java).setAction(BondVpnService.ACTION_STOP))
            } else {
                connect()
            }
        }

        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 2)
        }
    }

    private fun showMode() {
        val on = useServer.isChecked
        serverBox.visibility = if (on) View.VISIBLE else View.GONE
        modeHint.text = if (on) {
            "Every connection is split across all networks"
        } else {
            "Off: each new connection takes one network in turn"
        }
    }

    /** The code to start with: empty means no-server mode. */
    private fun activeCode(): String =
        if (useServer.isChecked) code.text.toString().trim() else ""

    private fun connect() {
        val c = code.text.toString().trim()
        if (useServer.isChecked && Mobile.serverHost(c).isEmpty()) {
            Toast.makeText(this, "Paste the connection code from your server first", Toast.LENGTH_LONG).show()
            return
        }
        getSharedPreferences("netbond", Context.MODE_PRIVATE).edit().putString("code", c).apply()
        val ask = VpnService.prepare(this)
        if (ask != null) {
            @Suppress("DEPRECATION")
            startActivityForResult(ask, 1)
        } else {
            startVpn()
        }
    }

    @Deprecated("Deprecated in Java")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == 1 && resultCode == RESULT_OK) startVpn()
    }

    private fun startVpn() {
        val i = Intent(this, BondVpnService::class.java)
            .setAction(BondVpnService.ACTION_START)
            .putExtra(BondVpnService.EXTRA_CODE, activeCode())
        startForegroundService(i)
    }

    override fun onResume() {
        super.onResume()
        handler.post(tick)
    }

    override fun onPause() {
        super.onPause()
        handler.removeCallbacks(tick)
    }

    private fun mbps(bytes: Long, ms: Long): String {
        if (ms <= 0) return "0.0 Mbps"
        val v = bytes * 8.0 / 1_000_000.0 / (ms / 1000.0)
        return String.format(Locale.US, "%.1f Mbps", v)
    }

    private fun size(bytes: Long): String = when {
        bytes >= 1L shl 30 -> String.format(Locale.US, "%.2f GB", bytes / 1073741824.0)
        bytes >= 1L shl 20 -> String.format(Locale.US, "%.1f MB", bytes / 1048576.0)
        else -> String.format(Locale.US, "%d KB", bytes / 1024)
    }

    private fun refresh() {
        val on = BondVpnService.running
        toggle.text = if (on) "Disconnect" else "Connect"
        toggle.setBackgroundResource(if (on) R.drawable.btn_disconnect else R.drawable.btn_connect)
        toggle.setTextColor(getColor(if (on) R.color.danger else R.color.bg))
        code.isEnabled = !on
        useServer.isEnabled = !on
        log.text = BondVpnService.logText()

        if (!on) {
            status.text = "Off"
            down.text = "0.0 Mbps"
            up.text = "0.0 Mbps"
            links.removeAllViews()
            prev.clear()
            prevAt = 0
            return
        }

        val now = System.currentTimeMillis()
        val dt = if (prevAt == 0L) 0L else now - prevAt
        prevAt = now
        var totalDown = 0L
        var totalUp = 0L
        var upCount = 0
        val direct = !useServer.isChecked
        links.removeAllViews()
        try {
            val arr = JSONObject(Mobile.stats()).getJSONArray("links")
            for (i in 0 until arr.length()) {
                val l = arr.getJSONObject(i)
                val id = l.getString("id")
                val tx = l.getLong("tx")
                val rx = l.getLong("rx")
                val isUp = l.getBoolean("up")
                val p = prev[id]
                val dTx = if (p == null) 0L else maxOf(0L, tx - p[0])
                val dRx = if (p == null) 0L else maxOf(0L, rx - p[1])
                prev[id] = longArrayOf(tx, rx)
                totalDown += dRx
                totalUp += dTx
                if (isUp) upCount++
                addLinkCard(
                    l.getString("name"),
                    when {
                        !isUp -> "Connecting…"
                        direct -> "Active"
                        else -> "Connected · ${l.getLong("rtt")} ms"
                    },
                    isUp,
                    "↓ ${mbps(dRx, dt)}   ↑ ${mbps(dTx, dt)}",
                    "${size(rx)} down · ${size(tx)} up"
                )
            }
        } catch (_: Exception) {
        }
        down.text = mbps(totalDown, dt)
        up.text = mbps(totalUp, dt)
        status.text = when (upCount) {
            0 -> if (direct) "On · waiting for a network" else "Connecting…"
            1 -> "On · 1 network in use"
            else -> if (direct) "On · sharing across $upCount networks" else "On · $upCount networks combined"
        }
    }

    private fun dp(v: Int): Int = (v * resources.displayMetrics.density).toInt()

    private fun addLinkCard(name: String, state: String, isUp: Boolean, speed: String, totals: String) {
        val card = LinearLayout(this)
        card.orientation = LinearLayout.VERTICAL
        card.setBackgroundResource(R.drawable.card)
        card.setPadding(dp(16), dp(14), dp(16), dp(14))
        val lp = LinearLayout.LayoutParams(
            LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT
        )
        lp.topMargin = dp(10)

        val top = LinearLayout(this)
        top.orientation = LinearLayout.HORIZONTAL
        val title = TextView(this)
        title.text = name
        title.textSize = 16f
        title.setTextColor(getColor(R.color.text))
        title.layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        val st = TextView(this)
        st.text = state
        st.textSize = 13f
        st.gravity = Gravity.END
        st.setTextColor(getColor(if (isUp) R.color.accent else R.color.muted))
        top.addView(title)
        top.addView(st)

        val sp = TextView(this)
        sp.text = speed
        sp.textSize = 14f
        sp.setTextColor(getColor(R.color.text))
        sp.setPadding(0, dp(6), 0, 0)

        val tot = TextView(this)
        tot.text = totals
        tot.textSize = 12f
        tot.setTextColor(getColor(R.color.muted))
        tot.setPadding(0, dp(2), 0, 0)

        card.addView(top)
        card.addView(sp)
        card.addView(tot)
        links.addView(card, lp)
    }
}
