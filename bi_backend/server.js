//npm install express cors playwright



const express = require('express');
const { chromium } = require('playwright');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());

const resultBasePath = "/Users/rshukla/Library/CloudStorage/OneDrive-InfobloxInc/hackthon-2024/Project/data_dreamers/RESULT";

// Function to ensure directory exists
const ensureDirectoryExistence = (filePath) => {
  const dirname = path.dirname(filePath);
  if (fs.existsSync(dirname)) {
    return true;
  }
  ensureDirectoryExistence(dirname);
  fs.mkdirSync(dirname);
};

// Function to extract data from Tableau
const extractDataFromTableau = async (dashboardUrl) => {
    const browser = await chromium.connectOverCDP('http://localhost:9222');
    const context = browser.contexts()[0];
    const page = await context.newPage();
  
    try {
      await page.goto(dashboardUrl);
      await page.waitForResponse(response => response.url().includes('runtimeanimweb.wasm'), { timeout: 300000 });
  
      // Increase timeout for waitForSelector
      await page.waitForSelector('button[id="download"]', { timeout: 60000 });
      await page.click('button[id="download"]');
      await page.waitForTimeout(1000);
  
      await page.waitForSelector('div[id="viz-viewer-toolbar-download-menu"]', { timeout: 60000 });
      await page.waitForTimeout(1000);
  
      const downloadOptions = await page.$$('div[data-tb-test-id^="download-flyout-download-crosstab"]');
      for (const downloadOption of downloadOptions) {
        await downloadOption.click();
        await page.waitForTimeout(1000);
  
        const sheetThumbnail = await page.waitForSelector('div[aria-selected="true"][data-tb-test-id^="sheet-thumbnail"]', { timeout: 60000 });
        const itemIndex = await sheetThumbnail.getAttribute('data-itemindex');
  
        const exportButton = await page.waitForSelector('button[data-tb-test-id="export-crosstab-export-Button"]', { timeout: 60000 });
        await exportButton.click();
        await page.waitForTimeout(1000);
  
        const data = await page.evaluate(() => {
          const rows = Array.from(document.querySelectorAll('.tabCrosstab .tabTable > tbody > tr'));
          return rows.map(row => {
            const cells = row.querySelectorAll('td');
            return Array.from(cells).map(cell => cell.innerText);
          });
        });
  
        const csvContent = data.map(row => row.join(',')).join('\n');
        const fileName = `crosstab-sheet${itemIndex}.csv`;
        const dashboardName = dashboardUrl.split('/').pop();
        const dashboardPath = path.join(resultBasePath, dashboardName);
        ensureDirectoryExistence(dashboardPath);
        fs.writeFileSync(path.join(dashboardPath, fileName), csvContent);
      }
    } catch (error) {
      console.error('Error extracting data:', error);
    } finally {
      await page.close();
    }
  };
  
  

app.post('/extract', async (req, res) => {
  const { url } = req.body;
  if (!url) {
    return res.status(400).json({ message: 'URL is required' });
  }

  try {
    await extractDataFromTableau(url);
    res.json({ message: 'Data extraction started' });
  } catch (error) {
    console.error(`Error: ${error}`);
    res.status(500).json({ message: 'Error extracting data' });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
