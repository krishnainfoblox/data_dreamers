// server.js
const express = require('express');
const { chromium } = require('playwright');
const cors = require('cors');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const os = require('os');

const app = express();
const port = 3001;

app.use(cors());
app.use(express.json());


const result_file_path = "/Users/rshukla/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/code-hackathon-2024/result/BI_performance.xlsx";

// Function to ensure directory exists
const ensureDirectoryExistence = (filePath) => {
  const dirname = path.dirname(filePath);
  if (fs.existsSync(dirname)) {
    return true;
  }
  ensureDirectoryExistence(dirname);
  fs.mkdirSync(dirname);
};

app.post('/test-performance', async (req, res) => {
  const { url } = req.body;

  if (!url) {
    return res.status(400).json({ error: 'URL is required' });
  }

  let browser, page;

  try {
    browser = await chromium.connectOverCDP('http://localhost:9222');
    const context = browser.contexts()[0];
    page = await context.newPage();

    const startTime = Date.now();
    await page.goto(url);
    await page.waitForResponse(response => response.url().includes('runtimeanimweb.wasm'), { timeout: 300000 });

    const navigationEndTime = Date.now();
    const navigationTimeTaken = navigationEndTime - startTime;

    const performanceMetrics = await page.evaluate(() => {
      function calculateSpeedIndex(resources) {
        let runningSum = 0;
        let lastVisuallyComplete = 0;

        resources.forEach(resource => {
          if (resource.responseEnd > lastVisuallyComplete) {
            lastVisuallyComplete = resource.responseEnd;
          }
          runningSum += resource.responseEnd - resource.startTime;
        });

        return runningSum / lastVisuallyComplete;
      }

      const navigation = performance.getEntriesByType('navigation')[0];
      const resources = performance.getEntriesByType('resource');

      const dnsLookupTime = navigation.domainLookupEnd - navigation.domainLookupStart;
      const tcpConnectTime = navigation.connectEnd - navigation.connectStart;
      const requestResponseTime = navigation.responseEnd - navigation.requestStart;
      const domContentLoadedTime = navigation.domContentLoadedEventEnd - navigation.domContentLoadedEventStart;
      const loadEventTime = navigation.loadEventEnd - navigation.loadEventStart;
      const totalLoadTime = navigation.loadEventEnd - navigation.startTime;
      const speedIndex = calculateSpeedIndex(resources);

      return {
        dnsLookupTime,
        tcpConnectTime,
        requestResponseTime,
        domContentLoadedTime,
        loadEventTime,
        totalLoadTime,
        speedIndex
      };
    });

    const timestamp = new Date().toISOString();
    const result = {
      url,
      timestamp,
      navigationTimeTaken,
      ...performanceMetrics
    };

    // Ensure the directory exists
    ensureDirectoryExistence(result_file_path);

    let workbook;
    let worksheet;
    let existingData = [];

    // Check if file exists
    if (fs.existsSync(result_file_path)) {
      // Read the existing file
      workbook = xlsx.readFile(result_file_path);
      worksheet = workbook.Sheets['Performance Metrics'];
      existingData = xlsx.utils.sheet_to_json(worksheet);
    } else {
      // Create a new workbook
      workbook = xlsx.utils.book_new();
    }

    // Append new result
    existingData.push(result);
    worksheet = xlsx.utils.json_to_sheet(existingData);

    // Append or update the worksheet in the workbook
    if (workbook.SheetNames.includes('Performance Metrics')) {
      workbook.Sheets['Performance Metrics'] = worksheet;
    } else {
      xlsx.utils.book_append_sheet(workbook, worksheet, 'Performance Metrics');
    }

    xlsx.writeFile(workbook, result_file_path);

    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  } finally {
    if (page) {
      await page.close();
    }
    if (browser) {
      await browser.close();
    }
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
