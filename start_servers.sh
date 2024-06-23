#!/bin/bash

# Function to run a command in a new terminal window (MacOS specific using osascript)
run_in_terminal() {
    osascript -e "tell application \"Terminal\" to do script \"$1\""
}

# Function to check if a process is running
is_process_running() {
    pgrep -x "$1" > /dev/null
}

# Function to kill processes by port
kill_process_on_port() {
    PORT=$1
    PID=$(lsof -t -i:$PORT)
    if [[ -n "$PID" ]]; then
        kill -9 $PID
        echo "Killed process $PID on port $PORT"
    fi
}

# Kill processes using conflicting ports
kill_process_on_port 3001
kill_process_on_port 5173

# Start React development server function
start_react_server() {
    cd frontend
    npm install
    npm run dev &
    REACT_PID=$!
    cd ..
}

# Start Express server function
start_express_server() {
    cd backend
    npm install
    node server.js &
    EXPRESS_PID=$!
    cd ..
}

# Start bi_backend server function
start_bi_backend_server() {
    cd bi_backend
    npm install
    node server.js &
    BI_BACKEND_PID=$!
    cd ..
}

# Option 1: Start servers in separate terminals (MacOS specific)
start_servers_in_terminals() {
    run_in_terminal "cd frontend && npm install && npm run dev"
    run_in_terminal "cd backend && npm install && node server.js"
    run_in_terminal "cd bi_backend && npm install && node server.js"
}

# Option 2: Start servers in the background
start_servers_in_background() {
    start_react_server
    start_express_server
    start_bi_backend_server
}

# Choose how to start servers (uncomment one option)
# start_servers_in_terminals  # Uncomment to start servers in separate terminals
start_servers_in_background # Uncomment to start servers in the background

# Wait for all servers to start (adjust time as needed)
sleep 7

# Activate virtual environment (adjust the path to your virtual environment)
# source /path/to/your/venv/bin/activate

# Ensure Flask is installed
pip install flask

# Start Flask application
python3 app.py

# When Flask exits, kill the servers
if [[ -n $REACT_PID ]]; then
    kill $REACT_PID
fi

if [[ -n $EXPRESS_PID ]]; then
    kill $EXPRESS_PID
fi

if [[ -n $BI_BACKEND_PID ]]; then
    kill $BI_BACKEND_PID
fi
