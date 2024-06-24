var stopwatchInterval;
var startTime;
var resultDisplayed = false;

function updateTime() {
    var currentTime = new Date();
    document.getElementById('system_time').innerHTML = currentTime.toLocaleString();
}

function startStopwatch() {
    startTime = new Date().getTime();var startTime;
var stopwatchInterval;
var resultDisplayed = false;

function updateTime() {
    var currentTime = new Date();
    document.getElementById('system_time').textContent = currentTime.toLocaleString();
}

function startStopwatch() {
    startTime = new Date().getTime();
    stopwatchInterval = setInterval(function() {
        var elapsed = ((new Date()).getTime() - startTime) / 1000;
        document.getElementById('elapsed_time').textContent = elapsed.toFixed(2) + " seconds";
    }, 100);
}

function stopStopwatch() {
    clearInterval(stopwatchInterval);
    var elapsed = ((new Date()).getTime() - startTime) / 1000;
    document.getElementById('elapsed_time').textContent = elapsed.toFixed(2) + " seconds";
}

window.onload = function() {
    updateTime();
    setInterval(updateTime, 1000);

    if (document.querySelector('.result')) {
        resultDisplayed = true;
        stopStopwatch();
    }

    document.getElementById('validationForm').addEventListener('submit', function(event) {
        event.preventDefault(); // Prevent form submission for now

        // Start stopwatch when form is submitted
        startStopwatch();

        // Simulate processing delay (you may replace this with actual form submission logic)
        setTimeout(function() {
            stopStopwatch(); // Stop stopwatch after processing is done
            // Update other parts of your page as needed
        }, 3000); // Example delay of 3 seconds
    });
};

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


document.addEventListener('DOMContentLoaded', function() {
    const systemTimeElement = document.getElementById('system_time');
    const elapsedTimeElement = document.getElementById('elapsed_time');
    const validationForm = document.getElementById('validationForm');
    const resultDisplay = document.getElementById('resultDisplay');

    function updateSystemTime() {
        const now = new Date();
        systemTimeElement.textContent = now.toLocaleTimeString();
    }

    function showElapsedTime(startTime) {
        const endTime = new Date();
        const elapsedTime = (endTime - startTime) / 1000; // elapsed time in seconds
        elapsedTimeElement.textContent = elapsedTime + ' seconds';
    }

    // Update system time every second
    setInterval(updateSystemTime, 1000);

    validationForm.addEventListener('submit', function(event) {
        const startTime = new Date();
        elapsedTimeElement.textContent = 'Calculating...';
        resultDisplay.textContent = 'Processing...';

        // You can remove event.preventDefault() to allow the form to submit and trigger the Flask route
    });
});
