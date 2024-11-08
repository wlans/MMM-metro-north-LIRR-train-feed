/* MagicMirror²
 * Module: MMM-metro-north-LIRR-train-feed
 * A generic transit parser to display upcoming departures
 * for a selected set of lines
 *
 * By Wyatt Lansdale. Credit to Ben Nitkin for transitfeeds
 * MIT Licensed.
 */

// to download GTFS file
const Log = require("logger");
const NodeHelper = require("node_helper");

module.exports = NodeHelper.create(
    {
        // Subclassed functions
        start: function () {
            console.log(this.name + ' helper method started...'); /*eslint-disable-line*/
            this.busy = false;
        },

        socketNotificationReceived: async function (notification, payload) {
            // SQLite (the DB backing gtfs) isn't safe across concurrent
            // or multithread programs - it'll reuse the
            // same memory buffers for new queries and cause corruption.
            // Handling one request at a time from the top level prevents that.
            // This isn't a very good semaphore, but JS isn't actually multithreaded
            // so any operation that isn't explicitly `async` stays atomic.
            while (this.busy) await new Promise(r => setTimeout(r, 100));
            this.busy = true;

            Log.log("MMM-metro-north-LIRR-train-feed: helper recieved", notification, payload);
            if (notification === 'GTFS_STARTUP') await this.startup(payload);
            if (notification === 'GTFS_QUERY_SEARCH') await this.query(payload.gtfs_config, payload.query);
            if (notification === 'GTFS_BROADCAST') await this.broadcast();

            this.busy = false;
        },

        startup: async function (gtfs_config) {
            this.gtfs = await import('gtfs');

            this.watch = [];
            // Import the data. Send a notification when ready.
            if (this.gtfs_config === undefined) {
                Log.log("MMM-metro-north-LIRR-train-feed: Importing with " + gtfs_config);
                this.gtfs_config = gtfs_config
                await this.gtfs.importGtfs(this.gtfs_config);
                this.gtfs.getRoutes({}, ['route_long_name', 'route_id']);
                Log.log("MMM-metro-north-LIRR-train-feed: Done importing!");

                // Start broadcasting the stations & routes we're watching.
                setInterval(() => this.broadcast(), 1000 * 60 * 1);
                setInterval(() => this.gtfs.updateGtfsRealtime(this.gtfs_config), 1000 * 60 * 5);
            }

            // Send a ready message now that we're loaded.
            this.sendSocketNotification("GTFS_READY", null);
        },

        query: async function (gtfs_config, query) {
            query.stops = {};
            query.routes = {};

            const routes = await this.gtfs.getRoutes({}, ['route_long_name', 'route_id']);
            for (const route of routes) {
                if (query.route_name === undefined || route.route_id.includes(query.route_name) || route.route_long_name.includes(query.route_name)) {
                    const stops = await this.gtfs.getStops({ route_id: route.route_id }, ['stop_name', 'stop_id']);
                    for (const stop of stops) {
                        if (stop.stop_name.includes(query.stop_name)) {
                            query.stops[stop.stop_id] = stop;
                            query.routes[route.route_id] = route;
                        }
                    }
                }
            }
            Log.log("MMM-metro-north-LIRR-train-feed: evaluated", query);
            this.watch.push(query);
        },
        broadcast: async function () {
            const start_time = Date.now()
            let results = {};
            realtime_count = 0;
            for (query of this.watch) {
                for (const [_stop_id, stop] of Object.entries(query.stops)) {
                    for (const [_route_id, route] of Object.entries(query.routes)) {
                        // Find all trips for the route
                        const trips = this.gtfs.getTrips({ route_id: route.route_id }, ['trip_id', 'direction_id', 'trip_headsign', 'service_id']);
                        for (trip of trips) {
                            if (query.direction === undefined || query.direction == trip.direction_id) {
                                // Now we have the stop and all the trips.
                                const stopDays = this.gtfs.getCalendarDates({ service_id: trip.service_id });
                                // const stopDays = this.gtfs.getCalendars({ service_id: trip.service_id });
                                const stoptime = this.gtfs.getStoptimes({ trip_id: trip.trip_id, stop_id: stop.stop_id }, ['departure_time', 'stop_sequence']);

                                // If stopDays is undefined, the calendar lookup failed.
                                // This happens if transit agencies use a calendar ("Summer", "Day after thanksgiving")
                                // without defining it in calendar.txt.
                                if (stopDays.length == 0) continue;
                                // If there's no stoptime, the train skips this stop.
                                if (stoptime.length == 0) continue;

                                const stopDatetimes = makeStopDatetimes(stopDays[0], stoptime[0].departure_time);
                                for (datetime of stopDatetimes) {
                                    const stop_delay = this.getRealtimeDelay(route.route_id, trip.trip_id, stop.stop_sequence, datetime);
                                    if (stop_delay !== null) {
                                        realtime_count += 1;
                                        Log.log(route.route_id, " is ", stop_delay, " late");
                                    }

                                    results[trip.trip_id + "@" + datetime] =
                                        JSON.parse(JSON.stringify({
                                            // IDs for tracing
                                            stop_id: stop.stop_id,
                                            route_id: route.route_id,
                                            trip_id: trip.trip_id,

                                            route_name: route.route_long_name,
                                            trip_terminus: trip.trip_headsign,
                                            direction: trip.direction_id,
                                            stop_name: stop.stop_name,
                                            stop_time: datetime,
                                            stop_delay: stop_delay,
                                        }));
                                    //Log.log(route.route_long_name + " towards " + trip.trip_headsign + " at " + datetime + " #" + trip.trip_id + " @ " + stop.stop_name);
                                }
                            }
                        }
                    }
                }
            }

            results = Object.values(results);

            // Now we have everything we need.
            Log.log("MMM-metro-north-LIRR-train-feed: Sending " + results.length + " trips; "
                + realtime_count + " have realtime data; processed in "
                + (Date.now() - start_time) + "ms");
            this.sendSocketNotification("GTFS_QUERY_RESULTS", results);
        },
        getRealtimeDelay: function (route_id, trip_id, stop_sequence, stop_time) {
            // Only look for realtime data if the vehicle's within an hour and up to 10m late.
            delay = null;
            const stopUpdates = this.gtfs.getStopTimeUpdates({ route_id: route_id });
            /* Updates have this form/fields:
            [{
                "trip_id": "CHE_719_V26_M",
                "route_id": null,
                "stop_id": "90719",
                "stop_sequence": 3,
                "arrival_delay": 180,
                "departure_delay": null,
                "departure_timestamp": null,
                "arrival_timestamp": "1970-01-01T00:00:00.000Z",
                "isUpdated": 1
            }]
             */
            var bestSequence = -1;
            // Find the update that's closest to our stop sequence
            // (but no greater)
            for (stopTimeUpdate of stopUpdates) {
                if (!stopTimeUpdate.trip_id.includes(trip_id) &&
                    !trip_id.includes(stopTimeUpdate.trip_id))
                    continue;
                if (stopTimeUpdate.stopSequence < bestSequence ||
                    stopTimeUpdate.stopSequence > stop_sequence)
                    continue;
                bestSequence = stopTimeUpdate.stopSequence;
                delay = delayFromStopTimeUpdate(stop_time, stopTimeUpdate);
            }
            //if (stopUpdates) Log.log(route_id, stopUpdates);
            return delay;
        },
    })

function delayFromStopTimeUpdate(stop_time, update) {
    // stopTimeUpdate can format delay in terms of adjusted
    // arrival or departure; and as a delay in seconds
    // or a new time. This works through all those options, preferring
    // departure time to arrival and seconds-delay to a new time.
    //
    // This returns the vehicle delay in seconds, or
    // `null` if delay couldn't be calculated.
    var delay = null;
    if (update.arrival_timestamp)
        delay = ((update.arrival_timestamp - stop_time) / 1000).toFixed();
    if (update.departure_timestamp)
        delay = ((update.departure_timestamp - stop_time) / 1000).toFixed();
    // Ignore non-credible delays. SEPTA publishes "arrival_timestamp": "1970-01-01T00:00:00.000Z" sometimes.
    if (Math.abs(delay) > 3600 * 12)
        delay = null;

    if (update.arrival_delay)
        delay = update.arrival_delay;
    if (update.departure_delay)
        delay = update.departure_delay;

    if (isNaN(delay))
        return null;
    return delay;
}
function makeStopDatetimes(calendar_date, stop_time) {
    const departures = [];
    const currentDate = new Date();
    const time = new Date('2000-01-01T' + stop_time);  // Parse time without a specific date

    console.log("Current date:", currentDate);
    console.log("Stop time:", stop_time);
    console.log("calendar_date:", calendar_date);  // Log single calendar_date object for debugging

    // Ensure calendar_date has a valid date property
    if (!calendar_date.date) {
        console.error("No date found in calendar_date");
        return departures;
    }

    // Parse the date from YYYYMMDD format
    const year = parseInt(calendar_date.date.toString().slice(0, 4));
    const month = parseInt(calendar_date.date.toString().slice(4, 6)) - 1;  // Months are zero-indexed
    const day = parseInt(calendar_date.date.toString().slice(6, 8));

    const departureDate = new Date(year, month, day);

    // Check if the date is within the next 3 days
    console.log(departureDate)
    const daysDifference = (departureDate - currentDate) / (1000 * 60 * 60 * 24);
    if (daysDifference >= 0 && daysDifference <= 3) {
        // Set the stop_time on the date
        departureDate.setHours(time.getHours());
        departureDate.setMinutes(time.getMinutes());
        departureDate.setSeconds(time.getSeconds());

        console.log("Adding departure:", departureDate);  // Debugging log
        departures.push(departureDate);
    } else {
        console.log("Skipping date:", departureDate);  // Debugging log for dates outside range
    }

    console.log("Final departures array:", departures);  // Log final list of departures
    return departures;
}




// function makeStopDatetimes(stop_days, stop_time) {
//     // This accepts a GTFS calendar and a stop time
//     // and creates Date objects for the next few days of arrivals.
//     // Start/end dates are ignored for calendars, as are holidays.
//     //
//     // Format reference
//     //stop_time: '09:43:00'
//     //stop_days: {
//     //   service_id: 'M3',
//     //   monday: 0,
//     //   tuesday: 0,
//     //   wednesday: 0,
//     //   thursday: 0,
//     //   friday: 0,
//     //   saturday: 0,
//     //   sunday: 1,
//     //   start_date: 20220821,
//     //   end_date: 20230304
//     // },

//     const departures = [];

//     // Create a Date with today's date and stop_time for the time.
//     const dateCandidate = new Date(Date.now());
//     const time = new Date('2000-01-01T' + stop_time)
//     dateCandidate.setHours(time.getHours());
//     dateCandidate.setMinutes(time.getMinutes());
//     dateCandidate.setSeconds(time.getSeconds());

//     const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];

//     for (let i = 0; i < 2; i++) {
//         const dayofweek = days[dateCandidate.getDay()];
//         if (stop_days[dayofweek] == 1)
//             departures.push(new Date(dateCandidate));

//         dateCandidate.setDate(dateCandidate.getDate() + 1)
//     }
//     return departures;
// }
