const fs = require('fs');

const now = Date.now();

async function fetchAndUpdatePrices() {
  const actual = await fetchJson('https://rest.fnar.net/exchange/all', []);
  const json = JSON.parse(fs.readFileSync('all.json', 'utf-8'));
  let current = [];

  for (const item of actual) {
    const existing = json.find(x => x.MaterialTicker === item.MaterialTicker && x.ExchangeCode === item.ExchangeCode);
    if (existing) {
      current.push(existing);
    } else {
      current.push({ ...item });
    }
  }

  const sorted = current.slice().sort((a, b) => {
    const dateA = a.Timestamp ? new Date(a.Timestamp) : new Date(0);
    const dateB = b.Timestamp ? new Date(b.Timestamp) : new Date(0);
    return dateA - dateB;
  });

  let rateLimited = false;
  for (const item of sorted) {
    const fullTicker = getFullTicker(item);
    const upToDateItem = actual.find(x => getFullTicker(x) === fullTicker);
    Object.assign(item, upToDateItem);

    if (!isStaleTimestamp(item.Timestamp) || rateLimited) {
      console.log('OK: ' + fullTicker);
      continue;
    }

    console.log('UPDATING: ' + fullTicker);
    const rawCxpc = await Promise.race([
      fetchJson('https://rest.fnar.net/exchange/cxpc/' + fullTicker, []),
      timeout(3000),
    ]);
    if (rawCxpc === undefined) {
      console.log('RATE LIMITED');
      rateLimited = true;
      continue;
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

    const DAY_ONE = cxpc.DAY_ONE ?? [];
    let yesterday = DAY_ONE.filter(x => isInLast48To24Hours(x.DateEpochMs))[0];
    let last30Days = DAY_ONE.filter(x => isInLast30Days(x.DateEpochMs) && !isAnomalous(x));
    let last7Days = last30Days.filter(x => isInLast7Days(x.DateEpochMs));
    if (last30Days.length === 0) {
      last30Days = undefined;
    }
    if (last7Days.length === 0) {
      last7Days = undefined;
    }

    item.Timestamp = new Date().toISOString();
    item.FullTicker = fullTicker;
    item.OpenYesterday = yesterday?.Open;
    item.CloseYesterday = yesterday?.Close;
    item.HighYesterday = yesterday?.High;
    item.LowYesterday = yesterday?.Low;
    item.TradedYesterday = yesterday?.Traded;
    item.TWAP7D = formatNumber(twap(last7Days));
    item.VWAP7D = formatNumber(vwap(last7Days));
    item.Traded7D = last7Days?.map(x => x.Traded).reduce((x, y) => x + y, 0);
    item.AverageTraded7D = formatNumber(last7Days?.map(x => x.Traded).reduce((x, y) => x + y / 7, 0));
    item.TWAP30D = formatNumber(twap(last30Days));
    item.VWAP30D = formatNumber(vwap(last30Days));
    item.Traded30D = last30Days?.map(x => x.Traded).reduce((x, y) => x + y, 0);
    item.AverageTraded30D = formatNumber(last30Days?.map(x => x.Traded).reduce((x, y) => x + y / 30, 0));

    replaceUndefinedWithNull(item);
    await timeout(1000);
  }

  fs.writeFileSync('all.json', JSON.stringify(current, null, 2));
  fs.writeFileSync('all.csv', jsonToCsv(current));
  console.log('Prices updated successfully');
  process.exit(0);
}

function isAnomalous(day) {
  if (day.Traded === 0) {
    return true;
  }
  const max = Math.max(day.Open, day.Close);
  const min = Math.min(day.Open, day.Close);
  const factor = 10;
  return day.High > max * factor || day.Low < min / factor;
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

async function fetchJson(url, fallback) {
  const response = await fetch(url);
  if (response.status === 204) {
    return fallback;
  }
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch (error) {
    console.log('Failed to parse JSON');
    console.log(text);
    throw error;
  }
}

function timeout(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function jsonToCsv(jsonData) {
  const array = Array.isArray(jsonData) ? jsonData : [jsonData];
  const keys = Object.keys(array[0]).filter(x => x !== 'Timestamp');
  const csvRows = [keys.join(',')];

  for (const obj of array) {
    const values = keys.map(key => JSON.stringify(obj[key], (key, value) => {
      if (typeof value === 'number') {
        return Math.round(value * 100) / 100;
      }
      return value === null ? '' : value;
    }));
    csvRows.push(values.join(','));
  }

  return csvRows.join('\n');
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

  let totalPrice = 0;
  let totalDays = 0;

  for (const day of data) {
    const price = (day.Open + day.Close + day.High + day.Low) / 4;
    totalPrice += price;
    totalDays++;
  }

  return totalDays > 0 ? totalPrice / totalDays : undefined;
}

function vwap(data) {
  if (!data) {
    return undefined;
  }

  let totalMoney = 0;
  let totalAmount = 0;

  for (const day of data) {
    totalMoney += day.Volume;
    totalAmount += day.Traded;
  }

  return totalAmount > 0 ? totalMoney / totalAmount : undefined;
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
