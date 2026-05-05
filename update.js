const fs = require('fs');

const now = Date.now();
const DAYS_BACK = 32;

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

  const sinceTimestamp = now - DAYS_BACK * 24 * 60 * 60 * 1000;
  const cxpcFull = await fetchJson('https://rest.fnar.net/exchange/cxpc/full/DAY_ONE/' + sinceTimestamp, []);
  const entriesByTicker = new Map();
  for (const group of cxpcFull) {
    const fullTicker = group.MaterialTicker + '.' + group.ExchangeCode;
    entriesByTicker.set(fullTicker, group.Entries ?? []);
  }

  for (const item of current) {
    const ticker = item.MaterialTicker;
    const exchange = item.ExchangeCode;
    const fullTicker = getFullTicker(item);
    const upToDateItem = actual.find(x => getFullTicker(x) === fullTicker);
    Object.assign(item, upToDateItem);

    const DAY_ONE = (entriesByTicker.get(fullTicker) ?? []).slice().sort(compareEntries);
    let yesterday = DAY_ONE.filter(x => isInLast48To24Hours(x.DateEpochMs))[0];
    let last30Days = DAY_ONE.filter(x => isInLast30Days(x.DateEpochMs) && !isAnomalous(ticker, exchange, x));
    let last7Days = last30Days.filter(x => isInLast7Days(x.DateEpochMs) && !isAnomalous(ticker, exchange, x));
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
  }

  fs.writeFileSync('all.json', JSON.stringify(current, null, 2));
  fs.writeFileSync('all.csv', jsonToCsv(current));
  console.log('Prices updated successfully');
  process.exit(0);
}

function compareEntries(a, b) {
  if (a.DateEpochMs < b.DateEpochMs) {
    return 1;
  }
  if (a.DateEpochMs > b.DateEpochMs) {
    return -1;
  }
  return 0;
}

const incidents = {
  // https://discord.com/channels/350171287785701388/359623296993722368/1500892559747186860
  '04.05.2026 SF Dupe': {
    '*.AI1': [1777852800000],
  },
  // https://discord.com/channels/350171287785701388/350171288267915277/1501245484407328829
  '05.05.2026 SF Dupe Part II': {
    '*.IC1': [1777939200000],
  },
}

const anomalousDays = new Set();
for (const incident of Object.values(incidents)) {
  for (const [ticker, timestamps] of Object.entries(incident)) {
    for (const timestamp of timestamps) {
      anomalousDays.add(`${ticker}${timestamp}`);
    }
  }
}

function isAnomalous(ticker, exchange, day) {
  if (day.Traded === 0) {
    return true;
  }
  const ts = day.DateEpochMs;
  const patterns = [ticker + '.' + exchange, `*.${exchange}`, `${ticker}.*`, '*'];
  if (patterns.some(p => anomalousDays.has(`${p}${ts}`))) {
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
