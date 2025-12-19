const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const OUTPUT_PATH = process.env.BUYERS_GUIDE_OUTPUT || path.join(process.cwd(), 'buyers_guide_output.csv');
const START_URL = process.env.BUYERS_GUIDE_URL || 'https://example.com/buyers-guide';
const MAX_RETRIES = Number(process.env.BUYERS_GUIDE_RETRIES || 3);

const CSV_COLUMNS = [
  'vehicle',
  'engine',
  'skpPart',
  'skpUrl',
  'skpDescription',
  'skpSpecs',
  'interchangePart',
  'interchangeUrl',
  'interchangeDescription',
  'interchangeSpecs',
];

function toCsvValue(value) {
  const safe = value == null ? '' : String(value);
  if (safe.includes('"') || safe.includes(',') || safe.includes('\n')) {
    return `"${safe.replace(/"/g, '""')}"`;
  }
  return safe;
}

function toCsvRow(data) {
  return CSV_COLUMNS.map((column) => toCsvValue(data[column] || '')).join(',');
}

async function withRetries(label, action, retries = MAX_RETRIES) {
  let lastError;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      return await action(attempt);
    } catch (error) {
      lastError = error;
      console.warn(`[retry:${label}] attempt ${attempt} failed: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
    }
  }
  throw lastError;
}

async function getPrimaryTable(page) {
  const tableByHeaders = page.locator('table', {
    has: page.locator('th', { hasText: /vehicle/i }),
    has: page.locator('th', { hasText: /engine/i }),
  });
  if ((await tableByHeaders.count()) > 0) {
    return tableByHeaders.first();
  }
  const tableByText = page.locator('table', { hasText: /vehicle/i });
  if ((await tableByText.count()) > 0) {
    return tableByText.first();
  }
  return null;
}

async function getHeaderIndex(table, regex) {
  const headers = table.locator('th');
  const count = await headers.count();
  for (let i = 0; i < count; i += 1) {
    const text = (await headers.nth(i).innerText()).trim();
    if (regex.test(text)) {
      return i;
    }
  }
  return null;
}

async function getRowCellText(row, index) {
  const cells = row.locator('td');
  if ((await cells.count()) === 0) {
    return '';
  }
  const cell = cells.nth(index);
  return (await cell.innerText()).trim();
}

async function expandRowIfNeeded(row) {
  const toggle = row.locator('[aria-expanded="false"], button:has-text("Expand"), button:has-text("Details"), button:has-text("Show")');
  if ((await toggle.count()) > 0) {
    await toggle.first().click({ timeout: 5000 }).catch(() => null);
  }
}

async function findPartLink(row, matcher) {
  const anchor = row.locator('a', { hasText: matcher });
  if ((await anchor.count()) > 0) {
    return anchor.first();
  }
  const button = row.locator('button', { hasText: matcher });
  if ((await button.count()) > 0) {
    return button.first();
  }
  return null;
}

async function resolveHref(handle, baseUrl) {
  if (!handle) {
    return null;
  }
  const href = await handle.evaluate((node) => node.getAttribute('href') || node.dataset?.href || null).catch(() => null);
  if (!href) {
    return null;
  }
  try {
    return new URL(href, baseUrl).toString();
  } catch (error) {
    return href;
  }
}

async function openInfoPage(context, url) {
  const listingPage = await context.newPage();
  await listingPage.goto(url, { waitUntil: 'domcontentloaded' });

  const infoLocator = listingPage.locator('a,button', { hasText: /info/i });
  if ((await infoLocator.count()) === 0) {
    return { infoPage: listingPage, listingPage, infoUrl: listingPage.url() };
  }

  let infoPage = null;
  await Promise.all([
    context.waitForEvent('page').then((page) => {
      infoPage = page;
    }).catch(() => null),
    infoLocator.first().click({ timeout: 5000 }).catch(() => null),
  ]);

  if (infoPage) {
    await infoPage.waitForLoadState('domcontentloaded');
    return { infoPage, listingPage, infoUrl: infoPage.url() };
  }

  await listingPage.waitForLoadState('domcontentloaded');
  return { infoPage: listingPage, listingPage, infoUrl: listingPage.url() };
}

async function extractLabeledValue(page, labelRegex) {
  const labelLocator = page.locator('text=/./').filter({ hasText: labelRegex }).first();
  if ((await labelLocator.count()) === 0) {
    return '';
  }
  const value = await labelLocator.evaluate((node) => {
    const clean = (text) => (text || '').replace(/\s+/g, ' ').trim();
    const siblings = [node.nextElementSibling, node.parentElement?.nextElementSibling];
    for (const sibling of siblings) {
      if (sibling && sibling.textContent) {
        return clean(sibling.textContent);
      }
    }
    const row = node.closest('tr');
    if (row) {
      const cells = row.querySelectorAll('td');
      if (cells.length > 0) {
        return clean(cells[cells.length - 1].textContent);
      }
    }
    const container = node.closest('div,section,article');
    if (container) {
      const valueEl = container.querySelector('[data-testid*="value"], .value, dd');
      if (valueEl) {
        return clean(valueEl.textContent);
      }
    }
    return '';
  }).catch(() => '');

  return value;
}

async function extractSpecs(page) {
  const specs = {};
  const specTable = page.locator('table', { hasText: /spec/i });
  if ((await specTable.count()) > 0) {
    const rows = specTable.first().locator('tr');
    const rowCount = await rows.count();
    for (let i = 0; i < rowCount; i += 1) {
      const row = rows.nth(i);
      const cells = row.locator('th,td');
      if ((await cells.count()) >= 2) {
        const key = (await cells.nth(0).innerText()).trim();
        const value = (await cells.nth(1).innerText()).trim();
        if (key) {
          specs[key] = value;
        }
      }
    }
  }

  const dtNodes = page.locator('dt');
  const dtCount = await dtNodes.count();
  for (let i = 0; i < dtCount; i += 1) {
    const key = (await dtNodes.nth(i).innerText()).trim();
    const value = (await dtNodes.nth(i).evaluate((node) => node.nextElementSibling?.textContent || '')).trim();
    if (key) {
      specs[key] = value;
    }
  }

  return specs;
}

function formatSpecs(specs) {
  return Object.entries(specs)
    .map(([key, value]) => `${key}: ${value}`)
    .join(' | ');
}

async function extractPartInfo(context, baseUrl, linkHandle) {
  if (!linkHandle) {
    return {
      part: '',
      url: '',
      description: '',
      specs: '',
    };
  }

  const partText = (await linkHandle.innerText()).trim();
  const linkUrl = await resolveHref(linkHandle, baseUrl);
  if (!linkUrl) {
    return { part: partText, url: '', description: '', specs: '' };
  }

  return withRetries(`part-info:${partText}`, async () => {
    const { infoPage, listingPage, infoUrl } = await openInfoPage(context, linkUrl);
    const description = await extractLabeledValue(infoPage, /description/i);
    const specs = await extractSpecs(infoPage);
    const specsText = formatSpecs(specs);

    if (infoPage !== listingPage) {
      await infoPage.close().catch(() => null);
    }
    await listingPage.close().catch(() => null);

    return {
      part: partText,
      url: infoUrl,
      description,
      specs: specsText,
    };
  });
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto(START_URL, { waitUntil: 'domcontentloaded' });

  const table = await getPrimaryTable(page);
  let rowsLocator = null;
  if (table) {
    rowsLocator = table.locator('tbody tr');
  }

  if (!rowsLocator || (await rowsLocator.count()) === 0) {
    rowsLocator = page.locator('[data-testid="buyers-guide-row"], tr');
  }

  const rowCount = await rowsLocator.count();
  const results = [];
  const vehicleIndex = table ? await getHeaderIndex(table, /vehicle/i) : null;
  const engineIndex = table ? await getHeaderIndex(table, /engine/i) : null;

  for (let i = 0; i < rowCount; i += 1) {
    const row = rowsLocator.nth(i);
    await expandRowIfNeeded(row);

    const vehicle = vehicleIndex != null ? await getRowCellText(row, vehicleIndex) : (await row.innerText()).split('\n')[0]?.trim() || '';
    const engine = engineIndex != null ? await getRowCellText(row, engineIndex) : (await row.innerText()).split('\n')[1]?.trim() || '';

    const skpLink = await findPartLink(row, /skp/i);
    const interchangeLink = await findPartLink(row, /interchange/i);

    const [skpInfo, interchangeInfo] = await Promise.all([
      extractPartInfo(context, page.url(), skpLink),
      extractPartInfo(context, page.url(), interchangeLink),
    ]);

    results.push({
      vehicle,
      engine,
      skpPart: skpInfo.part,
      skpUrl: skpInfo.url,
      skpDescription: skpInfo.description,
      skpSpecs: skpInfo.specs,
      interchangePart: interchangeInfo.part,
      interchangeUrl: interchangeInfo.url,
      interchangeDescription: interchangeInfo.description,
      interchangeSpecs: interchangeInfo.specs,
    });
  }

  const outputLines = [CSV_COLUMNS.join(','), ...results.map(toCsvRow)];
  fs.writeFileSync(OUTPUT_PATH, outputLines.join('\n'));

  await browser.close();
  console.log(`Wrote ${results.length} rows to ${OUTPUT_PATH}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
