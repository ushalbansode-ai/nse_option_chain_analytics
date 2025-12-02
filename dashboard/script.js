async function loadData() {
    let jsonURL = "../data/signal.json";

    // GitHub Pages support
    if (location.hostname.includes("github.io")) {
        const repo = location.pathname.split("/")[1];
        jsonURL = `https://${location.hostname}/${repo}/data/signal.json`;
    }

    try {
        const res = await fetch(jsonURL + "?v=" + Date.now());
        const data = await res.json();
        renderTable(data);
    } catch (e) {
        console.error("Loading failed", e);
    }
}

function renderTable(data) {
    document.getElementById("updated").innerText = data.timestamp;

    const tbody = document.querySelector("#signalTable tbody");
    tbody.innerHTML = "";

    Object.keys(data.signals).forEach(symbol => {
        const s = data.signals[symbol];

        const row = `
            <tr>
                <td>${symbol}</td>
                <td>${s.trend}</td>
                <td>${s.strength}</td>
                <td>${s.volume_spike}</td>
                <td>${s.reversal_signal}</td>
                <td>${s.premium_discount}</td>
                <td>${s.last_price}</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

loadData();

