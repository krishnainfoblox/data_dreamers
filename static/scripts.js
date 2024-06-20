var stopwatchInterval;
var startTime;
var resultDisplayed = false;

function updateTime() {
    var currentTime = new Date();
    document.getElementById('system_time').innerHTML = currentTime.toLocaleString();
}

function startStopwatch() {
    startTime = new Date().getTime();
    stopwatchInterval = setInterval(function() {
        if (!resultDisplayed) {
            var elapsed = ((new Date()).getTime() - startTime) / 1000;
            document.getElementById('elapsed_time').innerHTML = elapsed.toFixed(2) + " seconds";
        }
    }, 100);
}

function stopStopwatch() {
    clearInterval(stopwatchInterval);
    var elapsed = ((new Date()).getTime() - startTime) / 1000;
    document.getElementById('elapsed_time').innerHTML = elapsed.toFixed(2) + " seconds";
}

window.onload = function() {
    updateTime();
    setInterval(updateTime, 1000);

    if (document.querySelector('.result')) {
        resultDisplayed = true;
        stopStopwatch();
    }
};
