// src/App.js
import React, { useState } from 'react';
import axios from 'axios';
import './App.css';  // Import the CSS file

function App() {
  const [url, setUrl] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [darkMode, setDarkMode] = useState(false);

  const metricDescriptions = {
    dnsLookupTime: "The time taken for DNS resolution.",
    tcpConnectTime: "The time taken to establish a TCP connection.",
    requestResponseTime: "The time taken from sending the request to receiving the full response.",
    domContentLoadedTime: "The time when the DOMContentLoaded event completes.",
    loadEventTime: "The time when the load event completes.",
    totalLoadTime: "The total time taken from initiating the navigation to the load event completion.",
    speedIndex: "A metric that represents how quickly content is visually displayed during page load.",
    navigationAndLoadingTime: "The overall time taken for navigation and initial loading of the page."
  
  };

  const handleSubmit = async () => {
    setLoading(true);
    try {
      const response = await axios.post('http://localhost:3001/test-performance', { url });
      setResult(response.data);
      window.open(url, '_blank');
    } catch (error) {
      console.error('Error:', error);
      setResult({ error: error.message });
    }
    setLoading(false);
  };

  const toggleDarkMode = () => {
    setDarkMode(!darkMode);
  };

  return (
    <div className={`container ${darkMode ? 'dark-mode' : ''}`}>
      <h1>Tableau Performance Test</h1>
      <input 
        type="text" 
        value={url} 
        onChange={(e) => setUrl(e.target.value)} 
        placeholder="Enter Tableau Dashboard URL" 
      /> <br /><br />
      <button onClick={handleSubmit} disabled={loading}>
        {loading ? 'Testing...' : 'Run Performance Test'}
      </button><br /><br />
      <div className="tooltip">
        <span className="tooltip-text">{result && 'Hover over metrics for details'}</span>
        <h2>Performance Metrics</h2><br /><br />
      </div>
      {result && (
        <div>
          <ul>
            <li>
              <strong>DNS Lookup Time: {result.dnsLookupTime}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.dnsLookupTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>TCP Connect Time: {result.tcpConnectTime}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.tcpConnectTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>Request to Response Time: {result.requestResponseTime}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.requestResponseTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>DOM Content Loaded Time: {result.domContentLoadedTime}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.domContentLoadedTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>Load Event Time: {result.loadEventTime}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.loadEventTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>Total Load Time: {result.totalLoadTime}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.totalLoadTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>Speed Index: {result.speedIndex}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.speedIndex}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
            <li>
              <strong>Navigation and loading time: {result.navigationTimeTaken}ms </strong>
              <div className="tooltip">
                <span className="tooltip-text">{metricDescriptions.navigationAndLoadingTime}</span>
                <span className="tooltip">&#9432;</span>
              </div>
            </li>
          </ul>
        </div>
      )}
      <div className="switch">
        <input type="checkbox" id="toggle" checked={darkMode} onChange={toggleDarkMode} />
        <span className="slider"></span>
      </div>
      <label htmlFor="toggle">  Dark Mode  </label>
    </div>
  );
}

export default App;
