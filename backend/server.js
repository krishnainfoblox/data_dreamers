// server.js
const express = require('express');
const { chromium } = require('playwright');
const cors = require('cors'); // Import cors

const app = express();
const port = 3001;

// Use cors middleware
app.use(cors());
app.use(express.json());

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

    res.json({
      navigationTimeTaken,
      ...performanceMetrics
    });
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
