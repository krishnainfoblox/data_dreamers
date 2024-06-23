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


    const dashboardName = dashboardUrl.split('/').pop();
    const dashboardPath = path.join(resultBasePath, dashboardName);
    ensureDirectoryExistence(dashboardPath);

    const tabs = await page.$$('.tab');
    for (const [tabIndex, tab] of tabs.entries()) {
      await tab.click();
      await page.waitForTimeout(1000);

      const crosstabs = await page.$$('.tabToolbarButtonCrosstab');
      for (const [crossTabIndex, crosstab] of crosstabs.entries()) {
        await crosstab.click();
        await page.waitForTimeout(1000);

        const data = await page.evaluate(() => {
          const rows = Array.from(document.querySelectorAll('.tabCrosstab .tabTable > tbody > tr'));
          return rows.map(row => {
            const cells = row.querySelectorAll('td');
            return Array.from(cells).map(cell => cell.innerText);
          });
        });

        const csvContent = data.map(row => row.join(',')).join('\n');
        const fileName = `tab${tabIndex + 1}-crosstab${crossTabIndex + 1}.csv`;
        fs.writeFileSync(path.join(dashboardPath, fileName), csvContent);
      }
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
