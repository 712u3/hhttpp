const frontDb = {};

const output = document.getElementById("output");
const socket = new WebSocket("ws://localhost:8081/ws");
const states = ["from_storage", "wait_thread", "from_thread", "make_request", "error", "from_request"];
const icons = {
    "from_storage": "cloud_queue",
    "wait_thread": "vertical_align_bottom",
    "from_thread": "vertical_align_top",
    "make_request": "arrow_downward",
    "error": "close",
    "from_request": "arrow_upward",
};

const colors = {
    "from_storage": "green1",
    "wait_thread": "yellow1",
    "from_thread": "green1",
    "make_request": "yellow1",
    "error": "red1",
    "from_request": "green1",
};

socket.onopen = function () {
    console.log("Connected")
};

const addNewRequest = (obj) => {
    const div = document.createElement("div");
    div.innerHTML = obj.DestHost + obj.DestPath;
    div.id = obj.BirthTime;
    div.classList.add("div-el1");
    output.appendChild(div);
};

const getIconFor = (name) => {
    const i = document.createElement("i");
    i.classList.add("material-icons");
    i.innerHTML = icons[name];
    return i;
};

const addStateToRequest = (obj) => {
    const el = document.getElementById(obj.BirthTime);
    if (!el) {
        console.log("fatal error addStateToRequest");
        return;
    }
    const span = document.createElement("span");

    // span.innerHTML = obj.Source;
    span.appendChild(getIconFor(obj.Source));
    span.classList.add("my-status");
    span.classList.add(colors[obj.Source]);


    if (el.firstChild) {
        el.insertBefore(span, el.firstChild)
    } else {
        el.appendChild(span);
    }
};

socket.onmessage = function (e) {
    const obj = JSON.parse(e.data);

    if (!frontDb[obj.BirthTime] && !states.includes(obj.Source)) {
        frontDb[obj.StorageKey] = obj;
        addNewRequest(obj);
    } else {
        addStateToRequest(obj);
    }

    console.log(frontDb);

    // output.innerHTML += "Server: " + e.data + "\n";
};
