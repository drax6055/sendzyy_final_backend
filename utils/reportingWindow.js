/**
 * Reporting Window Utilities
 * Calculates timezone-accurate day boundaries using native Node.js Intl.DateTimeFormat.
 * Fully DST-safe across transition days (e.g., America/New_York, Europe/London) and zero external dependencies.
 */

/**
 * Gets the UTC offset in milliseconds for a specific instant and IANA timezone.
 * @param {Date} date - The specific timestamp
 * @param {string} timeZone - Valid IANA timezone string (e.g. 'Asia/Kolkata', 'America/New_York', 'UTC')
 * @returns {number} Offset in milliseconds (e.g. +19800000 for IST)
 */
function getTimezoneOffsetMs(date, timeZone) {
    try {
        const dtf = new Intl.DateTimeFormat('en-US', {
            timeZone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            fractionalSecondDigits: 3,
            hour12: false
        });
        const parts = dtf.formatToParts(date);
        const map = {};
        for (const p of parts) map[p.type] = p.value;

        const hour = map.hour === '24' ? 0 : parseInt(map.hour, 10);
        const asUtc = Date.UTC(
            parseInt(map.year, 10),
            parseInt(map.month, 10) - 1,
            parseInt(map.day, 10),
            hour,
            parseInt(map.minute, 10),
            parseInt(map.second, 10),
            parseInt(map.fractionalSecond || 0, 10)
        );
        return asUtc - date.getTime();
    } catch (err) {
        console.warn(`[reportingWindow] Invalid timezone '${timeZone}', falling back to UTC:`, err.message);
        return 0;
    }
}

/**
 * Computes exact start-of-day (00:00:00.000) UTC timestamp for a given YYYY-MM-DD in a timezone.
 */
function getStartOfDayUtc(year, month, day, timeZone) {
    const approx = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
    const offset1 = getTimezoneOffsetMs(approx, timeZone);
    const candidate = new Date(approx.getTime() - offset1);
    const offset2 = getTimezoneOffsetMs(candidate, timeZone);
    return new Date(approx.getTime() - offset2);
}

/**
 * Returns [startUtc, endUtc] for "yesterday" (or a specific relative day offset)
 * in the tenant's configured WABA reporting timezone, aligning daily buckets with Meta Business Manager.
 * 
 * @param {string} timezone - IANA timezone (default 'Asia/Kolkata')
 * @param {Date} [referenceDate] - Reference date (defaults to now)
 * @param {number} [dayOffset=-1] - Relative day offset (-1 for yesterday, 0 for today)
 * @returns {{ startUtc: Date, endUtc: Date, startUnix: number, endUnix: number, dateStr: string, timezone: string }}
 */
function getYesterdayWindowInTz(timezone = 'Asia/Kolkata', referenceDate = new Date(), dayOffset = -1) {
    const safeTz = timezone || 'Asia/Kolkata';

    // 1. Get reference date formatted in the target timezone
    let dtf;
    try {
        dtf = new Intl.DateTimeFormat('en-US', {
            timeZone: safeTz,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour12: false
        });
    } catch (_) {
        dtf = new Intl.DateTimeFormat('en-US', {
            timeZone: 'UTC',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour12: false
        });
    }

    const parts = dtf.formatToParts(referenceDate);
    const map = {};
    for (const p of parts) map[p.type] = p.value;

    const refYear = parseInt(map.year, 10);
    const refMonth = parseInt(map.month, 10);
    const refDay = parseInt(map.day, 10);

    // 2. Apply day offset in local calendar space
    const localCalDate = new Date(Date.UTC(refYear, refMonth - 1, refDay));
    localCalDate.setUTCDate(localCalDate.getUTCDate() + dayOffset);

    const targetYear = localCalDate.getUTCFullYear();
    const targetMonth = localCalDate.getUTCMonth() + 1;
    const targetDay = localCalDate.getUTCDate();

    // 3. Next day for boundary
    const nextCalDate = new Date(Date.UTC(targetYear, targetMonth - 1, targetDay + 1));
    const nextYear = nextCalDate.getUTCFullYear();
    const nextMonth = nextCalDate.getUTCMonth() + 1;
    const nextDay = nextCalDate.getUTCDate();

    // 4. Calculate exact start and end UTC instants
    const startUtc = getStartOfDayUtc(targetYear, targetMonth, targetDay, safeTz);
    const nextDayStartUtc = getStartOfDayUtc(nextYear, nextMonth, nextDay, safeTz);
    const endUtc = new Date(nextDayStartUtc.getTime() - 1); // 23:59:59.999

    const dateStr = `${targetYear}-${String(targetMonth).padStart(2, '0')}-${String(targetDay).padStart(2, '0')}`;

    return {
        startUtc,
        endUtc,
        startUnix: Math.floor(startUtc.getTime() / 1000),
        endUnix: Math.floor(endUtc.getTime() / 1000),
        dateStr,
        timezone: safeTz
    };
}

module.exports = {
    getYesterdayWindowInTz,
    getTimezoneOffsetMs,
    getStartOfDayUtc
};
