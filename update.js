const fs = require('fs');

const now = Date.now();

async function fetchAndUpdatePrices() {
  const actual = await fetchJson('https://rest.fnar.net/exchange/all');
  const prices = JSON.parse(fs.readFileSync('all.json', 'utf-8'));
  let wasUpdated = false;
  for (const item of prices) {
    const fullTicker = getFullTicker(item);
    if (!isStaleTimestamp(item.Timestamp)) {
      console.log('OK: ' + fullTicker);
      continue;
    }

    console.log('UPDATING: ' + fullTicker);
    const rawCxpc = await Promise.race([
      fetchJson('https://rest.fnar.net/exchange/cxpc/' + fullTicker),
      timeout(3000),
    ]);
    if (rawCxpc === undefined) {
      console.log('RATE LIMITED');
      break;
    }

    const cxpc = {};
    for (const entry of rawCxpc) {
      const values = cxpc[entry.Interval] ?? [];
      values.push(entry);
      cxpc[entry.Interval] = values;
    }

    function compare(a, b) {
      if (a.DateEpochMs < b.DateEpochMs) {
        return 1;
      }
      if (a.DateEpochMs > b.DateEpochMs) {
        return -1;
      }
      return 0;
    }

    for (const interval in cxpc) {
      const values = cxpc[interval];
      values.sort(compare);
    }

    const DAY_ONE = cxpc.DAY_ONE;
    let today = DAY_ONE?.[0];
    if (today && !isInLast24Hours(today.DateEpochMs)) {
      today = undefined;
    }
    let yesterday = DAY_ONE?.[1];
    if (yesterday && !isInLast48To24Hours(yesterday.DateEpochMs)) {
      yesterday = undefined;
    }
    let last7Days = (DAY_ONE ?? []).filter(x => isInLast7Days(x.DateEpochMs));
    if (last7Days.length === 0) {
      last7Days = undefined;
    }
    let last30Days = (DAY_ONE ?? []).filter(x => isInLast30Days(x.DateEpochMs));
    if (last30Days.length === 0) {
      last30Days = undefined;
    }

    const upToDateItem = actual.find(x => getFullTicker(x) === fullTicker);
    Object.assign(item, upToDateItem);
    item.Timestamp = new Date().toISOString();
    item.FullTicker = fullTicker;
    item.Open = today?.Open;
    item.Close = today?.Close;
    item.High = today?.High;
    item.Low = today?.Low;
    item.Traded = today?.Traded;
    item.OpenYesterday = yesterday?.Open;
    item.CloseYesterday = yesterday?.Close;
    item.HighYesterday = yesterday?.High;
    item.LowYesterday = yesterday?.Low;
    item.TradedYesterday = yesterday?.Traded;
    item.TWAP7D = formatNumber(twap(last7Days));
    item.Traded7D = last7Days?.map(x => x.Traded).reduce((x, y) => x + y, 0);
    item.AverageTraded7D = formatNumber(last7Days?.map(x => x.Traded).reduce((x, y) => x + y / 7, 0));
    item.TWAP30D = formatNumber(twap(last30Days));
    item.Traded30D = last30Days?.map(x => x.Traded).reduce((x, y) => x + y, 0);
    item.AverageTraded30D = formatNumber(last30Days?.map(x => x.Traded).reduce((x, y) => x + y / 30, 0));

    replaceUndefinedWithNull(item);
    wasUpdated = true;

    await timeout(1000);
  }
  if (wasUpdated) {
    fs.writeFileSync('all.json', JSON.stringify(prices, null, 2));
    fs.writeFileSync('all.csv', jsonToCsv(prices));
  }
  console.log('Prices updated successfully');
  process.exit(0);
}

function getFullTicker(item) {
  return item.MaterialTicker + '.' + item.ExchangeCode;
}

function isStaleTimestamp(timestamp) {
  if (!timestamp) {
    return true;
  }

  const existingTimestamp = new Date(timestamp);
  const oneDayAgo = new Date(now - 24 * 60 * 60 * 1000);
  return existingTimestamp < oneDayAgo;
}

async function fetchJson(url) {
  return await (await fetch(url)).json();
}

function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function jsonToCsv(jsonData) {
  const array = Array.isArray(jsonData) ? jsonData : [jsonData];
  const keys = Object.keys(array[0]);
  const csvRows = [keys.join(',')];

  for (const obj of array) {
    const values = keys.map(key => JSON.stringify(obj[key], (key, value) => value === null ? '' : value));
    csvRows.push(values.join(','));
  }

  return csvRows.join('\n');
}

function isInLast24Hours(timestamp) {
  const oneDay = 24 * 60 * 60 * 1000;
  return now - timestamp <= oneDay;
}

function isInLast48To24Hours(timestamp) {
  const oneDay = 24 * 60 * 60 * 1000;
  return now - timestamp <= 2 * oneDay && now - timestamp > oneDay;
}
function isInLast7Days(timestamp) {
  const oneWeek = 7 * 24 * 60 * 60 * 1000;
  return now - timestamp <= oneWeek;
}

function isInLast30Days(timestamp) {
  const oneMonth = 30 * 24 * 60 * 60 * 1000;
  return now - timestamp <= oneMonth;
}

function twap(data) {
  if (!data) {
    return undefined;
  }

  let totalTradedValue = 0;
  let totalVolume = 0;

  for (const day of data) {
    totalTradedValue += day.Traded * day.Close;
    totalVolume += day.Traded;
  }

  return totalVolume > 0 ? totalTradedValue / totalVolume : undefined;
}

function formatNumber(number) {
  return number ? Math.round(number * 100) / 100 : number;
}

function replaceUndefinedWithNull(obj) {
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      if (obj[i] === undefined) {
        obj[i] = null;
      } else if (typeof obj[i] === 'object' && obj[i] !== null) {
        replaceUndefinedWithNull(obj[i]);
      }
    }
  } else if (typeof obj === 'object' && obj !== null) {
    for (const key in obj) {
      if (obj[key] === undefined) {
        obj[key] = null;
      } else if (typeof obj[key] === 'object' && obj[key] !== null) {
        replaceUndefinedWithNull(obj[key]);
      }
    }
  }
}

void fetchAndUpdatePrices();