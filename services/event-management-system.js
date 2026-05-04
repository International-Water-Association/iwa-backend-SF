'use strict';

/**
 * @fileoverview Service module for handling event-related operations using jsforce.
 * This module provides functionality for managing events, attendees, tickets, and related data.
 * 
 * @module events-service
 * @requires jsforce
 * @requires @strapi/utils
 */

const jsforce = require('jsforce');
const { ApplicationError } = require('@strapi/utils').errors;

// ============================================================================
// Configuration Constants
// ============================================================================

/**
 * Cache duration constants in milliseconds
 * @constant {Object}
 */
const CACHE_TTL = {
    SHORT: 5 * 60 * 1000,   // 5 minutes
    MEDIUM: 15 * 60 * 1000, // 15 minutes
    LONG: 60 * 60 * 1000    // 1 hour
};

/**
 * Cache and batch processing limits
 * @constant {Object}
 */
const CACHE_LIMITS = {
    MAX_SIZE: 1000,  // Maximum number of cache entries
    BATCH_SIZE: 5    // Batch processing size for operations
};

/**
 * SQL query templates for common operations
 * @constant {Object}
 */
const SQL_QUERIES = {
    GET_ACTIVE_EVENTS: `
        SELECT 
            "SFID",
            "Name",
            "EventApi__Display_Name__c",
            "EventApi__Description__c",
            "EventApi__Image_URL__c",
            "EventApi__Status__c",
            "EventApi__Start_Date__c",
            "EventApi__End_Date__c",
            "EventApi__Event_Key__c"
        FROM events."EventApi__Event__c" 
        WHERE "EventApi__Status__c" = 'Active'
        ORDER BY "EventApi__Start_Date__c" DESC
        LIMIT 100
    `,
    // Add more query templates as needed
};
function toMultiPicklist(val) {
    if (Array.isArray(val)) return val.join(';');
    if (typeof val === 'string') return val;
    return null;
}
module.exports = () => ({
    /**
     * Establishes a connection to Salesforce using jsforce.
     * @param {string} request - The access token for Salesforce authentication
     * @returns {Promise<Object>} The jsforce connection object
     * @throws {ApplicationError} If connection fails
     */
    async getJSforceConnection(request) {
        try {
            return new jsforce.Connection({
                instanceUrl: process.env.SF_BASEURL,
                accessToken: request
            });
        } catch (error) {
            console.error('Error connecting to Salesforce:', error);
            throw new ApplicationError("Failed to connect to Salesforce", error);
        }
    },

    async syncEventsDB(key, sendEvent) {
        if (!key) {
            throw new ApplicationError('Invalid key provided');
        }
        try {
            sendEvent('info', "Connecting to Salesforce...");
            const conn = await this.getJSforceConnection(key);
            sendEvent('success', "Connected to Salesforce");

            const [
                { insertEventData, insertVenueData, insertTicketsData, insertPrData, insertPrValData },
                insertSrcCodeData
            ] = await Promise.all([
                this.getEvents(conn, sendEvent),
                this.getSourceCode(conn, sendEvent)
            ]);
            // Clear tables and reset sequences.
            sendEvent('info', "Connecting to Postgres DB...");
            const trx = await strapi.db.connection.transaction();

            try {

                sendEvent('success', "Connected to PG DB");

                sendEvent('info', "Locking tables...");
                await trx.raw(`
                    LOCK TABLE 
                        events."EventApi__Event__c",
                        events."EventApi__Venue__c",
                        events."EventApi__Ticket_Type__c",
                        events."OrderApi__Price_Rule__c",
                        events."OrderApi__Price_Rule_Variable__c",
                        events."OrderApi__Source_Code__c"
                    IN ACCESS EXCLUSIVE MODE;
                `);
                sendEvent('success', "Tables locked");

                sendEvent('info', "Clearing events tables...");
                await trx.raw(`
                    TRUNCATE 
                    events."EventApi__Event__c",
                    events."EventApi__Venue__c",
                    events."EventApi__Ticket_Type__c",
                    events."OrderApi__Price_Rule__c",
                    events."OrderApi__Price_Rule_Variable__c",
                    events."OrderApi__Source_Code__c"
                    RESTART IDENTITY CASCADE;
                `);
                sendEvent('success', "Tables cleared");

                if (insertEventData?.length) {
                    sendEvent('info', `Inserting ${insertEventData.length} events records...`);
                    await trx.batchInsert('events.EventApi__Event__c', insertEventData, 1000);
                    sendEvent('success', "events inserted");
                }

                if (insertVenueData?.length) {
                    sendEvent('info', `Inserting ${insertVenueData.length} venue records...`);
                    await trx.batchInsert('events.EventApi__Venue__c', insertVenueData, 1000);
                    sendEvent('success', "venue inserted");
                }

                if (insertSrcCodeData?.length) {
                    sendEvent('info', `Inserting ${insertSrcCodeData.length} source code records...`);
                    await trx.batchInsert('events.OrderApi__Source_Code__c', insertSrcCodeData, 1000);
                    sendEvent('success', "source code inserted");
                }

                if (insertTicketsData?.length) {
                    sendEvent('info', `Inserting ${insertTicketsData.length} ticket records...`);
                    await trx.batchInsert('events.EventApi__Ticket_Type__c', insertTicketsData, 1000);
                    sendEvent('success', "ticket inserted");
                }

                if (insertPrData?.length) {
                    sendEvent('info', `Inserting ${insertPrData.length} price rule records...`);
                    await trx.batchInsert('events.OrderApi__Price_Rule__c', insertPrData, 1000);
                    sendEvent('success', "price rule inserted");
                }

                if (insertPrValData?.length) {
                    sendEvent('info', `Inserting ${insertPrValData.length} Price Rule Variables records...`);
                    await trx.batchInsert('events.OrderApi__Price_Rule_Variable__c', insertPrValData, 1000);
                    sendEvent('success', "Price Rule Variables inserted");
                }


                await trx.commit();
                sendEvent('completed', "Events DB sync completed successfully");

            } catch (error) {
                await trx.rollback();
                sendEvent('warning', "The transaction did not go through due to a following processing issue. The system has safely cancelled the operation, and no impact has been made to existing records.");
                throw error;
            }

            return true;

        } catch (error) {
            sendEvent('error', `Error syncing events DB: ${error.message}`);
            console.error('syncEventsDB Error:', error);
            throw new ApplicationError('Error sync events DB', {
                originalError: error.message,
            });
        }
    },

    async getEvents(conn, sendEvent) {
        let insertEventData = [];
        let eventsId = [];

        let insertVenueData = [];

        try {
            sendEvent('info', "Fetching active events from Salesforce...");
            await new Promise((resolve, reject) => {
                conn.query(`select Id, EventApi__Event_Key__c, EventApi__Community_Group__c, EventApi__Description__c, EventApi__Display_Name__c, EventApi__End_Date__c,  Name, EventApi__Event_Category__c, Event_Type__c, EventApi__Image_URL__c, IsSfReg__c, EventApi__Overview_HTML__c, EventApi__Attendees__c, EventApi__Sold_Out__c, EventApi__Start_Date__c, EventApi__Status__c, EventApi__Capacity__c, EventApi__Ticket_Sales_Start_Date__c, EventApi__Quantity_Remaining__c, EventApi__Quantity_Sold__c, EventApi__Total_Event_Capacity__c, CurrencyIsoCode, terms_conditions__c, sender_email__c from EventApi__Event__c where IsSfReg__c = true`)
                    .on("record", function (events) {
                        eventsId.push(events.Id)
                        insertEventData.push({
                            Name: events.Name,
                            SFID: events.Id,
                            EventApi__Display_Name__c: events.EventApi__Display_Name__c,
                            EventApi__Description__c: events.EventApi__Description__c,
                            EventApi__Image_URL__c: events.EventApi__Image_URL__c,
                            EventApi__Overview_HTML__c: events.EventApi__Overview_HTML__c,
                            EventApi__Status__c: events.EventApi__Status__c,
                            EventApi__Start_Date__c: events.EventApi__Start_Date__c,
                            EventApi__End_Date__c: events.EventApi__End_Date__c,
                            CurrencyIsoCode: events.CurrencyIsoCode,
                            EventApi__Event_Key__c: events.EventApi__Event_Key__c,
                            terms_conditions__c: events.terms_conditions__c,
                            sender_email__c: events.sender_email__c || 'registration@iwahq.org',
                        })
                    })
                    .on("error", reject)
                    .on("end", resolve)
                    .run({ autoFetch: true, maxFetch: 500000 });
            });

            sendEvent('success', `Fetched ${insertEventData.length} Events records`);

            insertVenueData = await this.getVenu(eventsId, sendEvent, conn)
            const { insertTicketsData, insertPrData, insertPrValData } = await this.getTickets(eventsId, sendEvent, conn);

            return { insertEventData, insertVenueData, insertTicketsData, insertPrData, insertPrValData };

        } catch (error) {
            sendEvent('error', "Error fetching Events");
            console.error('SyncMembershipDB Error:', error);
            throw new ApplicationError("Err: Sync Events", error);
        }

    },

    async getVenu(eventsId, sendEvent, conn) {
        let insertVenueData = [];
        sendEvent('info', "Fetching venue details...");
        try {
            let clonedEventId = [...eventsId];
            while (clonedEventId.length > 0) {
                const chunk = clonedEventId.splice(0, 100);
                await new Promise((resolve, reject) => {
                    conn.query(`select Id, Name, EventApi__Event__c, EventApi__HTML_Description__c, EventApi__Is_Primary_Venue__c, EventApi__Image_URL__c, EventApi__Event_Location__c, EventApi__Website__c from EventApi__Venue__c where EventApi__Event__c  IN ('${chunk.join("','")}')`)
                        .on("record", function (venue) {
                            insertVenueData.push({
                                Name: venue.Name,
                                SFID: venue.Id,
                                EventApi__Event__c: venue.EventApi__Event__c,
                                EventApi__HTML_Description__c: venue.EventApi__HTML_Description__c,
                                EventApi__Is_Primary_Venue__c: venue.EventApi__Is_Primary_Venue__c,
                                EventApi__Image_URL__c: venue.EventApi__Image_URL__c,
                                EventApi__Event_Location__c: venue.EventApi__Event_Location__c,
                                EventApi__Website__c: venue.EventApi__Website__c,
                            })
                        })
                        .on("error", reject)
                        .on("end", resolve)
                        .run({ autoFetch: true, maxFetch: 5000000 });
                });
            }
            sendEvent('success', `Fetched ${insertVenueData.length} venue records`);
            return insertVenueData;
        } catch (error) {
            sendEvent('error', "Error fetching venue");
            console.error('SyncMembershipDB Error:', error);
            throw new ApplicationError("Err: Sync Events", error);
        }

    },

    async getTickets(eventsId, sendEvent, conn) {
        let insertTicketsData = [];
        let ticketID = []

        sendEvent('info', "Fetching Tickets details...");
        try {
            let clonedEventId = [...eventsId];
            while (clonedEventId.length > 0) {
                const chunk = clonedEventId.splice(0, 100);
                await new Promise((resolve, reject) => {
                    conn.query(`select Id, Name, EventApi__Event__c, EventApi__Description__c, EventApi__Ticket_Information__c, EventApi__Price__c, CurrencyIsoCode, Start_Date__c, End_Date__c, EventApi__Image_Path__c, EventApi__Maximum_Sales_Quantity__c, EventApi__Min_Sales_Quantity__c, EventApi__Is_Active__c, Ticket_Type__c from EventApi__Ticket_Type__c where EventApi__Event__c IN ('${chunk.join("','")}')`)
                        .on("record", function (tt) {
                            ticketID.push(tt.Id)
                            insertTicketsData.push({
                                Name: tt.Name,
                                SFID: tt.Id,
                                EventApi__Event__c: tt.EventApi__Event__c,
                                EventApi__Description__c: tt.EventApi__Description__c,
                                EventApi__Ticket_Information__c: tt.EventApi__Ticket_Information__c,
                                EventApi__Price__c: tt.EventApi__Price__c,
                                CurrencyIsoCode: tt.CurrencyIsoCode,
                                Start_Date__c: tt.Start_Date__c,
                                End_Date__c: tt.End_Date__c,
                                EventApi__Image_Path__c: tt.EventApi__Image_Path__c,
                                EventApi__Maximum_Sales_Quantity__c: tt.EventApi__Maximum_Sales_Quantity__c,
                                EventApi__Min_Sales_Quantity__c: tt.EventApi__Min_Sales_Quantity__c,
                                EventApi__Is_Active__c: tt.EventApi__Is_Active__c,
                                Ticket_Type__c: tt.Ticket_Type__c,
                            })
                        })
                        .on("error", reject)
                        .on("end", resolve)
                        .run({ autoFetch: true, maxFetch: 5000000 });
                });
            }
            sendEvent('success', `Fetched ${insertTicketsData.length} ticket records`);
        } catch (error) {
            sendEvent('error', "Error fetching venue");
            console.error('SyncMembershipDB Error:', error);
            throw new ApplicationError("Err: Sync Events", error);
        }

        const { insertPrData, insertPrValData } = await this.getPriceRule(ticketID, sendEvent, conn)

        return { insertTicketsData, insertPrData, insertPrValData };
    },

    async getPriceRule(ticketId, sendEvent, conn,) {
        let insertPrData = [];
        let insertPrValData = [];
        let priceruleId = [];
        try {
            sendEvent('info', "Fetching active Price Rules from Salesforce...");
            // First, load all active Price Rule records and keep their Ids.
            while (ticketId.length > 0) {
                const chunkTt = ticketId.splice(0, 100);
                await new Promise((resolve, reject) => {
                    conn.query(`select Id, OrderApi__Additional_Currencies_JSON__c, CurrencyIsoCode, OrderApi__Current_Num_Available__c, OrderApi__End_Date__c, OrderApi__Is_Active__c, OrderApi__Is_Promotional_Price__c, OrderApi__Item__c, OrderApi__Item_Class__c, FON_Item_Name__c, OrderApi__Limit_Per_Account__c, OrderApi__Limit_Per_Contact__c, OrderApi__Max_Assignments__c, OrderApi__Max_Num_Available__c, OrderApi__Max_Quantity__c, OrderApi__Min_Assignments__c, OrderApi__Min_Quantity__c, OrderApi__Num_Times_Used__c, OrderApi__Price__c, Name, OrderApi__Required_Badge_Types__c, OrderApi__Required_Source_Codes__c, OrderApi__Required_Subscription_Plans__c, OrderApi__Start_Date__c, EventApi__Ticket_Type__c, OrderApi__Tax_Inclusive_Price__c from OrderApi__Price_Rule__c where OrderApi__Is_Active__c = true and EventApi__Ticket_Type__c IN ('${chunkTt.join("','")}')`)
                        .on("record", function (pr) {
                            priceruleId.push(pr.Id)
                            insertPrData.push({
                                SFID: pr.Id,
                                OrderApi__Additional_Currencies_JSON__c: pr.OrderApi__Additional_Currencies_JSON__c,
                                CurrencyIsoCode: pr.CurrencyIsoCode,
                                OrderApi__Current_Num_Available__c: pr.OrderApi__Current_Num_Available__c,
                                OrderApi__End_Date__c: pr.OrderApi__End_Date__c,
                                OrderApi__Is_Active__c: pr.OrderApi__Is_Active__c,
                                OrderApi__Is_Promotional_Price__c: pr.OrderApi__Is_Promotional_Price__c,
                                OrderApi__Item__c: pr.OrderApi__Item__c,
                                OrderApi__Item_Class__c: pr.OrderApi__Item_Class__c,
                                FON_Item_Name__c: pr.FON_Item_Name__c,
                                OrderApi__Limit_Per_Account__c: pr.OrderApi__Limit_Per_Account__c,
                                OrderApi__Max_Assignments__c: pr.OrderApi__Max_Assignments__c,
                                OrderApi__Max_Num_Available__c: pr.OrderApi__Max_Num_Available__c,
                                OrderApi__Max_Quantity__c: pr.OrderApi__Max_Quantity__c,
                                OrderApi__Min_Assignments__c: pr.OrderApi__Min_Assignments__c,
                                OrderApi__Min_Quantity__c: pr.OrderApi__Min_Quantity__c,
                                OrderApi__Num_Times_Used__c: pr.OrderApi__Num_Times_Used__c,
                                OrderApi__Price__c: pr.OrderApi__Price__c,
                                Name: pr.Name,
                                OrderApi__Required_Badge_Types__c: pr.OrderApi__Required_Badge_Types__c,
                                OrderApi__Required_Source_Codes__c: pr.OrderApi__Required_Source_Codes__c,
                                OrderApi__Required_Subscription_Plans__c: pr.OrderApi__Required_Subscription_Plans__c,
                                OrderApi__Start_Date__c: pr.OrderApi__Start_Date__c,
                                EventApi__Ticket_Type__c: pr.EventApi__Ticket_Type__c,
                                OrderApi__Tax_Inclusive_Price__c: pr.OrderApi__Tax_Inclusive_Price__c
                            })
                        })
                        .on("error", reject)
                        .on("end", resolve)
                        .run({ autoFetch: true, maxFetch: 5000000 });
                });
            }
            sendEvent('success', `Fetched ${insertPrData.length} Price Rule records`);
            // Then, load Price Rule Variable records in manageable chunks per 100 Price Rule Ids.

            sendEvent('info', "Fetching Price Rule Variables...");
            while (priceruleId.length > 0) {
                const chunk = priceruleId.splice(0, 100);
                await new Promise((resolve, reject) => {
                    conn.query(`select Id, CurrencyIsoCode, OrderApi__Field__c, OrderApi__Object__c, OrderApi__Operator__c, Name, OrderApi__Value__c, OrderApi__Price_Rule__c from OrderApi__Price_Rule_Variable__c where OrderApi__Price_Rule__c IN ('${chunk.join("','")}')`)
                        .on("record", function (prVariable) {
                            insertPrValData.push({
                                CurrencyIsoCode: prVariable.CurrencyIsoCode,
                                OrderApi__Field__c: prVariable.OrderApi__Field__c,
                                OrderApi__Object__c: prVariable.OrderApi__Object__c,
                                OrderApi__Operator__c: prVariable.OrderApi__Operator__c,
                                Name: prVariable.Name,
                                OrderApi__Value__c: prVariable.OrderApi__Value__c,
                                OrderApi__Price_Rule__c: prVariable.OrderApi__Price_Rule__c,
                                SFID: prVariable.Id
                            })
                        })
                        .on("error", reject)
                        .on("end", resolve)
                        .run({ autoFetch: true, maxFetch: 5000000 });
                });
            }
            sendEvent('success', `Fetched ${insertPrValData.length} Price Rule Variable records`);

        } catch (error) {
            sendEvent('error', "Error fetching Price Rules / Variables");
            console.error('SyncMembershipDB Error:', error);
            throw new ApplicationError("Err: Sync price rule", error);
        }
        return { insertPrData, insertPrValData };
    },

    async getSourceCode(conn, sendEvent) {
        let insertSrcCodeData = [];
        try {
            // Stream all active Source Code records from Salesforce and collect
            // them into `insertData` for a single batch insert.
            sendEvent('info', "Fetching active Source Code from Salesforce...");
            await new Promise((resolve, reject) => {
                conn.query(`select Id, OrderApi__Active__c, Name from OrderApi__Source_Code__c where OrderApi__Active__c = true`)
                    .on("record", function (sourceCode) {
                        insertSrcCodeData.push({
                            OrderApi__Active__c: sourceCode.OrderApi__Active__c,
                            Name: sourceCode.Name,
                            SFID: sourceCode.Id
                        })
                    })
                    .on("error", reject)
                    .on("end", resolve)
                    .run({ autoFetch: true, maxFetch: 500000 });
            });

            sendEvent('success', `Fetched ${insertSrcCodeData.length} Source Code records`);

        } catch (error) {
            sendEvent('error', "Error fetching Source Code");
            console.error('Error:', error);
            throw new ApplicationError("Err: Sync Source Code", error);
        }
        return insertSrcCodeData;
    },



    // ============================================================================
    // Event Management Methods
    // ============================================================================

    /**
     * Retrieves all active events with pagination and caching.
     * @returns {Promise<Array>} Array of event objects
     * @throws {ApplicationError} If retrieval fails
     */
    async getAllEvents() {
        try {
            const result = await strapi.db.connection.context.raw(SQL_QUERIES.GET_ACTIVE_EVENTS);

            // Cache management
            if (result?.rows) {
                const cacheKey = `events_${result.rows.length}_${Date.now()}`;
                this.cache = this.cache || new Map();
                this.cache.set(cacheKey, {
                    data: result.rows,
                    timestamp: Date.now()
                });

                // Clean up old cache entries
                this._cleanupCache(this.cache);
            }

            return result?.rows || [];
        } catch (error) {
            console.error('Error in getAllEvents:', error);
            throw new ApplicationError("Failed to get events", error);
        }
    },

    /**
     * Gets basic event data with caching mechanism.
     * @param {string} request - Event key
     * @returns {Promise<Object>} Event data object
     * @throws {ApplicationError} If retrieval fails
     */
    async getBasicEventData(request) {
        try {
            // Cache check
            const cacheKey = `basic_event_${request}`;
            this.eventCache = this.eventCache || new Map();
            const cached = this.eventCache.get(cacheKey);

            if (cached && Date.now() - cached.timestamp < CACHE_TTL.SHORT) {
                return cached.data;
            }

            // Optimized query with specific columns and joins
            const query = `
                SELECT 
                    t1."SFID",
                    t1."Name",
                    t1."EventApi__Display_Name__c",
                    t1."EventApi__Description__c",
                    t1."EventApi__Image_URL__c",
                    t1."EventApi__Status__c",
                    t1."EventApi__Start_Date__c",
                    t1."EventApi__End_Date__c",
                    t1."EventApi__Event_Key__c",
                    t1."CurrencyIsoCode",
                    t1."terms_conditions__c",
                    t1."sender_email__c",
                    t2.page_data
                FROM events."EventApi__Event__c" t1
                INNER JOIN events."EventApi__PageData" t2 
                    ON t1."EventApi__Event_Key__c" = t2.event_key
                WHERE t1."EventApi__Event_Key__c" = ?
                AND t1."EventApi__Status__c" = 'Active'
                LIMIT 1`;

            const result = await strapi.db.connection.context.raw(query, [request]);

            // Cache management
            if (result?.rows?.[0]) {
                this.eventCache.set(cacheKey, {
                    data: result.rows[0],
                    timestamp: Date.now()
                });

                // Cache cleanup
                if (this.eventCache.size > CACHE_LIMITS.MAX_SIZE) {
                    const now = Date.now();
                    for (const [key, value] of this.eventCache.entries()) {
                        if (now - value.timestamp > CACHE_TTL.SHORT) {
                            this.eventCache.delete(key);
                        }
                    }
                }
            }

            return result?.rows?.[0] || null;
        } catch (error) {
            console.error('Error in getBasicEventData:', error);
            throw new ApplicationError("Failed to get basic event data", error);
        }
    },

    /**
     * Gets venue data for a given event key.
     * @param {string} request - The event key.
     * @returns {Promise<Object>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async getVenueData(request) {
        let result;
        try {
            let qry = `SELECT t1.* FROM events."EventApi__Venue__c" as t1 
                    join events."EventApi__Event__c" as t2 on t1."EventApi__Event__c" = t2."SFID" 
                    where t2."EventApi__Event_Key__c" = ? limit 1`;
            result = await strapi.db.connection.context.raw(qry, [request]);
        } catch (error) {
            throw new ApplicationError("Err: ", error);
        }
        return result.rows[0];
    },
    /**
     * Validates an email address against contact records with caching.
     * @param {string} request - Email address to validate
     * @returns {Object|null} Contact record if found, null otherwise
     * @throws {ApplicationError} If validation fails
     */
    async validateEmail(request) {
        try {
            // Input validation
            if (!request || typeof request !== 'string' || !request.includes('@')) {
                throw new ApplicationError("Invalid email format");
            }

            // Check cache first
            const cacheKey = `email_validation_${request.toLowerCase()}`;
            this.emailCache = this.emailCache || new Map();
            const cached = this.emailCache.get(cacheKey);

            if (cached && Date.now() - cached.timestamp < 300000) { // 5 minutes cache
                return cached.data;
            }

            // Optimized query with specific column selection and index hints
            const query = `
                SELECT 
                    email,
                    sfid,
                    zen_member_status__c,
                    fon_member_status__c
                FROM salesforce.contact 
                WHERE email = ?
                    AND zen_member_status__c is not null
                LIMIT 1`;

            const result = await strapi.db.connection.context.raw(query, [request.toLowerCase()]);
            const contact = result?.rows?.[0] || null;

            // Cache the result
            this.emailCache.set(cacheKey, {
                data: contact,
                timestamp: Date.now()
            });

            // Clean up old cache entries if cache size exceeds 1000
            if (this.emailCache.size > 1000) {
                const now = Date.now();
                for (const [key, value] of this.emailCache.entries()) {
                    if (now - value.timestamp > 300000) { // 5 minutes
                        this.emailCache.delete(key);
                    }
                }
            }

            return contact;
        } catch (error) {
            console.error('Error in validateEmail:', error);
            throw new ApplicationError("Failed to validate email", error);
        }
    },
    /**
     * Gets all countries with caching and optimized query.
     * @returns {Array} Array of country records
     * @throws {ApplicationError} If retrieval fails
     */
    async getAllCountry() {
        try {
            // Check cache first
            const cacheKey = 'all_countries';
            this.countryCache = this.countryCache || new Map();
            const cached = this.countryCache.get(cacheKey);

            // Cache for 1 hour since country data rarely changes
            if (cached && Date.now() - cached.timestamp < 3600000) {
                return cached.data;
            }

            // Optimized query with specific column selection and index hints
            const query = `
                SELECT 
                    fon_affiliate_association_lookup__c,
                    code__c,
                    name,
                    fon_country_code__c as code,
                    fon_iso_code__c,
                    fon_income_group__c
                FROM salesforce.fon_country_income_classification__c
                WHERE code__c IS NOT NULL
                ORDER BY name ASC
                /* Using index on code__c and name */`;

            const result = await strapi.db.connection.context.raw(query);

            if (result?.rows) {
                // Cache the result
                this.countryCache.set(cacheKey, {
                    data: result.rows,
                    timestamp: Date.now()
                });

                // No need for cache cleanup since we only have one key
            }

            return result.rows;
        } catch (error) {
            console.error('Error in getAllCountry:', error);
            throw new ApplicationError("Failed to get country list", error);
        }
    },
    /**
     * Gets sub events for a given event key.
     * @param {string} request - The event key.
     * @returns {Promise<Array>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async getSubEvent(request) {
        try {
            // Input validation
            if (!request) {
                throw new ApplicationError("Event key is required");
            }

            // Check cache first
            const cacheKey = `sub_event_${request}`;
            this.subEventCache = this.subEventCache || new Map();
            const cached = this.subEventCache.get(cacheKey);

            // Use cache if available and less than 5 minutes old
            if (cached && Date.now() - cached.timestamp < 300000) {
                return cached.data;
            }

            // Optimized query with specific column selection
            const query = `
                SELECT 
                    t1.*
                FROM events."EventApi__Ticket_Type__c" t1 
                INNER JOIN events."EventApi__Event__c" t2 
                    ON t2."SFID" = t1."EventApi__Event__c" 
                WHERE t2."EventApi__Event_Key__c" = ?
                    AND t1."Name" <> 'Delegate' 
                    AND t1."EventApi__Is_Active__c" = true
                /* Using index on EventApi__Event_Key__c and Name */
                ORDER BY t1."Start_Date__c" ASC`;

            const result = await strapi.db.connection.context.raw(query, [request]);

            // Cache the result
            if (result?.rows) {
                this.subEventCache.set(cacheKey, {
                    data: result.rows,
                    timestamp: Date.now()
                });

                // Clean up old cache entries if cache size exceeds 1000
                if (this.subEventCache.size > 1000) {
                    const now = Date.now();
                    for (const [key, value] of this.subEventCache.entries()) {
                        if (now - value.timestamp > 300000) { // 5 minutes
                            this.subEventCache.delete(key);
                        }
                    }
                }
            }

            return result?.rows || [];
        } catch (error) {
            console.error('Error in getSubEvent:', error);
            throw new ApplicationError("Failed to get sub events", error);
        }
    },

    // ============================================================================
    // Attendee Management Section
    // ============================================================================

    /**
     * Inserts a new attendee and their guests into the system.
     * Handles primary contact creation, guest processing, and ticket assignments.
     * 
     * @param {Object} request - Request object containing attendee details
     * @param {string} request.eventKey - Event identifier
     * @param {Object} request.primaryContact - Primary contact information
     * @param {Object} [request.guests] - Optional guest information
     * @returns {Promise<Object>} Updated request object with attendee IDs
     * @throws {ApplicationError} If insertion fails
     */
    async insertAttendee(request) {
        try {
            // Input validation
            if (!request.eventKey || !request.primaryContact) {
                throw new ApplicationError("Missing required fields: eventKey or primaryContact");
            }

            // Prepare all emails for batch checking
            const allEmails = [request.primaryContact.email];
            const guestEmails = request.guests ?
                Object.values(request.guests)
                    .map(g => g.email)
                    .filter(Boolean) : [];
            allEmails.push(...guestEmails);

            // Single database connection for entire operation
            const conn = await this.getJSforceConnection(request.accessToken);

            // Batch check for existing attendees
            const existingAttendees = await this.checkAttendeeEmail(
                allEmails,
                request.eventKey,
                request.accessToken
            );

            if (existingAttendees.length > 0) {
                throw new ApplicationError(
                    "Email(s) already registered",
                    existingAttendees
                );
            }

            // Process primary contact
            const PCData = await this.upsertContactSF(
                request.primaryContact,
                request.accessToken
            );
            if (!PCData) {
                throw new ApplicationError("Failed to create primary contact");
            }

            // Update primary contact data
            request.primaryContact.sfid = PCData;
            const formData = await this.upsertContactFormData(
                request.eventKey,
                request.primaryContact,
                null
            );

            if (!formData?.length) {
                throw new ApplicationError("Failed to create form data");
            }

            request.primaryContact.attId = formData[0].id;
            request.primaryContact.formDataId = formData[0].id;
          // Insert primary contact ticket
            await this.insertTicketToCart(
                formData[0].id,
                request.ticketID,
                request.eventKey,
                ''
            );

            // Process guests in batches if they exist
            if (request.guests) {
                const guestEntries = Object.entries(request.guests);
                // Process guests in batches for better performance
                for (let i = 0; i < guestEntries.length; i += CACHE_LIMITS.BATCH_SIZE) {
                    const batch = guestEntries.slice(i, i + CACHE_LIMITS.BATCH_SIZE);
                    await Promise.all(batch.map(async ([index, guest]) => {
                        try {
                            const guestId = await this.upsertContactSF(
                                guest,
                                request.accessToken
                            );

                            if (guestId) {
                                request.guests[index].sfid = guestId;
                                const guestFormData = await this.upsertContactFormData(
                                    request.eventKey,
                                    request.guests[index],
                                    request.primaryContact.attId
                                );
                                if (guestFormData?.length) {
                                    request.guests[index].attId = guestFormData[0].id;
                                    request.guests[index].formDataId = guestFormData[0].id;
                                    request.primaryContact.guests[index].formDataId = guestFormData[0].id;
                                    if (!guest?.is_accompany_ticket__c) {
                                        await this.insertTicketToCart(
                                            guestFormData[0].id,
                                            request.ticketID,
                                            request.eventKey,
                                            ''
                                        );
                                    }
                                }
                            }
                        } catch (error) {
                            console.error(`Error processing guest ${index}:`, error);
                            // Continue with other guests even if one fails
                        }
                    }));
                }
            }

            return request;
        } catch (error) {
            console.error('Error in insertAttendee:', error);
            throw new ApplicationError(
                error.message || "Failed to insert attendee",
                error
            );
        }
    },

    /** 
     * Inserts an activity log into the database.
     * @param {Object} request - The request object containing activity log details.
     * @returns {Promise<Object>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async activityLog(request) {
        try {
            var result =
                await strapi.db.connection.context.raw(
                    `INSERT INTO events."EventApi__Activity_Log"(email, step, data, event_id) VALUES (?, ?, ?, ?)`,
                    [
                        request.email,
                        request.step,
                        request.data,
                        request.event_id,
                    ]
                );

            return {
                success: true,
                message: "Activity log created successfully",
            };
        } catch (error) {
            console.error('Error in activityLog:', error);
            throw new ApplicationError("Failed to create activity log", error);
        }
    },
    /**
     * Upserts a contact in Salesforce.
     * @param {Object} contactData - The contact data object.
     * @param {string} accessToken - The access token for the Salesforce connection.
     * @returns {Promise<string>} The ID of the contact.
     * @throws {ApplicationError} If there is an error in the operation.
    */
    async upsertContactSF(contactData, accessToken) {
        const cacheKey = `contact_${contactData.email}`;
        const cache = this.cache || new Map();
        this.cache = cache;

        try {
            // Check cache first
            const cached = cache.get(cacheKey);
            if (cached && Date.now() - cached.timestamp < 300000) { // 5 minutes cache
                return cached.id;
            }

            var conn = await this.getJSforceConnection(accessToken);
            const dates = contactData.one_day_delegate_date__c;

            const formattedDates = Array.isArray(dates)
              ? dates.map((d) => {
                  const dateObj = d instanceof Date ? d : new Date(d);
                  return !isNaN(dateObj.getTime())
                    ? dateObj.toLocaleDateString('en-GB').replace(/\//g, '-')
                    : null;
                }).filter(Boolean)
              : [];
            // Prepare contact data
            const param = {
                FirstName: contactData.firstname,
                LastName: contactData.lastname,
                Email: contactData.email,
                FON_Dialling_Code__c: contactData.fon_dialling_code__c,
                MobilePhone: contactData.mobilephone,
                Student__c: contactData.student__c,
                IsProgramBookNeeded__c: contactData.isprogrambookneeded__c,
                FON_Organisation_Type__c: contactData.fon_organisation_type__c,
                Are_you_a_YWP__c: contactData.are_you_a_ywp__c,
                FON_Gender__c: contactData.fon_gender__c,
                Student_Card_URL__c: contactData.student_card_url__c,
                FON_Contact_Source__c: "Event",
                AccountId: contactData.accountid || "0014K000008d8BZQAY",
                FON_Income_Group__c: contactData.fon_income_group__c,
                Zen_Address_Line_1__c: contactData.zen_address_line_1__c,
                Zen_Address_Line_2__c: contactData.zen_address_line_2__c,
                Zen_Address_Line_3__c: contactData.zen_address_line_3__c,
                MailingStreet: (contactData.zen_address_line_1__c + " " + contactData.zen_address_line_2__c + " " + contactData.zen_address_line_3__c).replace(/null/g, '').replace(/undefined/g, ''),
                MailingPostalCode: contactData.mailingpostalcode,
                Mailingcity: contactData.mailingcity,
                MailingCountry: contactData.mailingcountry,
                MailingState: contactData.mailingstate,
                Zen_Billing_Address_Line_1__c: contactData.zen_billing_address_line_1__c,
                Zen_Billing_Address_Line_2__c: contactData.zen_billing_address_line_2__c,
                Zen_Billing_Address_Line_3__c: contactData.zen_billing_address_line_3__c,
                OtherStreet: (contactData.zen_billing_address_line_1__c + " " + contactData.zen_billing_address_line_2__c + " " + contactData.zen_billing_address_line_3__c).replace(/null/g, '').replace(/undefined/g, ''),
                OtherPostalCode: contactData.otherpostalcode,
                Othercity: contactData.othercity,
                OtherCountry: contactData.othercountry,
                OtherState: contactData.otherstate,
                Company__c: contactData.company__c,
                Current_or_Most_Recent_Job__c: contactData.current_or_most_recent_job__c,
                OrderApi__Preferred_Currency__c: "GBP",
                Dietary_Requirements__c: contactData.Dietary_Requirements__c,
                Allergies__c: Array.isArray(contactData?.Allergies__c)
                    ? contactData.Allergies__c.join(';')
                    : contactData?.Allergies__c ?? null,
                Dietary_Requirements_Others__c: contactData.Dietary_Requirements_Others__c,
                VISA__c: contactData.visa__c,
                Given_Name__c: contactData.Given_Name__c,
                Surname__c: contactData.Surname__c,
                Nationality__c: contactData.Nationality__c,
                Passport_Number__c: contactData.passport_number__c,
                Passport_Date_of_Birth__c: contactData.passport_date_of_birth__c,
                Issued_Date__c: contactData.issued_date__c,
                Expire_Date__c: contactData.expire_date__c,
                Terms_Conditions__c: contactData.Terms_Conditions__c,
                Data_protection_consent__c: contactData.Data_protection_consent__c,
                Communication_consent__c: false,

                Event_Billing_Section__CountryCode__s: contactData.companyCountryCode,
                Event_Billing_Section__StateCode__s: contactData.companystateCode,
                Event_Billing_Section__City__s: contactData.companycity,
                Event_Billing_Section__PostalCode__s: contactData.companypostalcode,
                Event_Billing_Section__Street__s: contactData.companystreet,

                Author_ID_Poster_Presenter__c: contactData.author_id_poster_presenter__c,
                Author_ID_Workshop_Presenter__c: contactData.author_id_workshop_presenter__c,
                Author_ID_only_for_speakers__c: contactData.author_id_only_for_speakers__c,
                Programme_Speaker__c: contactData.programme_speaker__c,
                Event_Photo_URL__c: contactData.fon_photo_url__c,
                is_accompany_ticket__c: contactData.is_accompany_ticket__c == null ? false : contactData.is_accompany_ticket__c,
                Is_one_day_registration__c: contactData.is_one_day_registration__c == null ? false : contactData.is_one_day_registration__c,
                one_day_delegate_date__c: formattedDates.join(','),
                //WH 2025
                WH_Company_Type__c: contactData.wh_company_type__c,
                //WH_Ticket_Type__c: contactData.wh_ticket_type__c,
            };

            // Check if contact exists
            const checkEmailQuery = `SELECT Id, Email FROM Contact WHERE Email = '${contactData.email}' LIMIT 1`;
            const checkEmailResult = await conn.query(checkEmailQuery);

            let result = [];
            if (checkEmailResult.records.length > 0) {
                param.Id = checkEmailResult.records[0].Id;
                const contact = await conn.sobject('Contact').update(param);
                if (contact) {
                    result = param.Id;
                }
                else {
                    console.error('Failed to update contact', contact);
                    throw new ApplicationError("Failed to update contact", contact);
                    return false;
                }
            } else {
                const contact = await conn.sobject('Contact').insert(param);
                if (contact && contact.id) {
                    result = contact.id;
                }
                else {
                    console.error('Failed to create contact', error);
                    throw new ApplicationError("Failed to create contact", error);
                    return false;
                }
            }

            // Cache the result
            cache.set(cacheKey, {
                id: result,
                timestamp: Date.now()
            });

            // Clean up old cache entries
            if (cache.size > 1000) {
                const now = Date.now();
                for (const [key, value] of cache.entries()) {
                    if (now - value.timestamp > 300000) {
                        cache.delete(key);
                    }
                }
            }

            return result;
        } catch (error) {
            console.error('Error in upsertContactSF:', error);
            throw new ApplicationError("Failed to create contact", error);
        }
    },
    /**
     * Updates form data for an attendee.
     * @param {Object} request - The request object containing attendee information.
     * @returns {Promise<Array>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async updateFormData(request) {
        let result = [];
        try {
            const formData = await this.upsertContactFormData(
                request.eventKey,
                request.primaryContact,
                null
            );

            if (formData.length > 0) {
                result = formData[0];
            }
        } catch (error) {
            console.error('Error in updateFormData:', error);
        }
        return result;
    },
    /**
     * Prepares additional ticket for an attendee.
     * @param {Object} request - The request object containing attendee information.
     * @returns {Promise<Array>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async prepareAdditionalTicket(request) {
        let result = [];
        try {
            const primaryContact = await strapi.db.connection.context.raw(
                `select id from events."EventApi__FormData__c" where (form_data->'order_details')::JSON->>'salesOrderId' =  ? LIMIT 1`,
                [request.salesOrderId]
            );

            if (primaryContact?.rows.length > 0) {
                const primaryContactId = primaryContact.rows[0].id;
                const newFormData = await strapi.db.connection.context.raw(
                    `WITH main_insert AS (
                        INSERT INTO events."EventApi__FormData__c" (
                            event_key, sfid, "name", email, primary_attendee_id, form_data, created_date, form_data_txt
                        )
                        SELECT
                            event_key,
                            sfid,
                            "name",
                            email,
                            NULL,  -- this is the main row
                            form_data,
                            created_date,
                            form_data_txt
                        FROM events."EventApi__FormData__c"
                        WHERE id = ?  
                        RETURNING id AS new_primary_id
                        ),

                        -- Step 2: Insert the child rows with updated primary_attendee_id pointing to the new main row
                        child_insert AS (
                        INSERT INTO events."EventApi__FormData__c" (
                            event_key, sfid, "name", email, primary_attendee_id, form_data, created_date, form_data_txt
                        )
                        SELECT
                            e.event_key,
                            e.sfid,
                            e."name",
                            e.email,
                            m.new_primary_id,  -- point to new main record
                            e.form_data,
                            e.created_date,
                            e.form_data_txt
                        FROM events."EventApi__FormData__c" e, main_insert m
                        WHERE e.primary_attendee_id::text = ?
                        ) SELECT * FROM main_insert;`,
                    [primaryContactId, primaryContactId]
                );

                if (newFormData?.rows?.[0]) {
                    result = newFormData.rows;
                }

            }
        } catch (error) {
            console.error('Error in prepareAdditionalTicket:', error);
            throw new ApplicationError("Failed to prepare additional ticket", error);
        }
        return result;
    },
    /**
     * Gets attendee by sales order id
     * @param {Object} request - The request object containing attendee information.
     * @returns {Promise<Array>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async getAttendeeBySalesOrderId(request) {
        let result = [];
        try {
            const primaryContact = await strapi.db.connection.context.raw(`select * from events."EventApi__FormData__c" where (form_data->'order_details')::JSON->>'salesOrderId' =  ? LIMIT 1`,
                [request.salesOrderId]);

            if (primaryContact?.rows.length > 0) {
                result = primaryContact.rows;
            }
        } catch (error) {
            console.error('Error in getAttendeeBySalesOrderId:', error);
            throw new ApplicationError("Failed to get attendee by sales order id", error);
        }
        return result;
    },

    async getFormData(request) {
        let result = [];
        try {
            const formData = await strapi.db.connection.context.raw(`select * from events."EventApi__FormData__c" where id = ? OR primary_attendee_id::text = ?`, [request.id, request.id]);
            result = formData.rows;
        } catch (error) {
            console.error('Error in getFormData:', error);
            throw new ApplicationError("Failed to get form data", error);
        }
        return result;
    },

    /**
     * Inserts or updates a contact form data record in the database.
     * @param {string} event_key - The event key.
     * @param {Object} data - The data object containing contact information.
     * @param {string} pa - The primary attendee ID.
     * @returns {Promise<Array>} The result of the operation.
    */
    async upsertContactFormData(event_key, data, pa) {
        let result = [];
        try {
            if (data.attId !== undefined && data.attId !== null && data.attId !== "") {
                let mainQuery = `UPDATE "events"."EventApi__FormData__c"
                SET event_key=?, sfid=?, name=?, email=?, primary_attendee_id=?, form_data=?
                WHERE id=? returning *`;
                result = await strapi.db.connection.context.raw(mainQuery, [
                    event_key,
                    data.sfid,
                    data.firstname + " " + data.lastname,
                    data.email,
                    pa,
                    data,
                    data.attId,
                ]);
            }
            else {
                let mainQuery =
                    `INSERT INTO "events"."EventApi__FormData__c"(event_key, sfid, name, email, primary_attendee_id, form_data) VALUES (?, ?, ?, ?, ?, ?) returning *`;
                result = await strapi.db.connection.context.raw(mainQuery, [
                    event_key,
                    data.sfid,
                    data.firstname + " " + data.lastname,
                    data.email,
                    pa,
                    data,
                ]);
            }
        } catch (error) {
            console.error('Error in upsertContactFormData:', error);
        }
        return result.rows
    },
    /**
     * Inserts a ticket into the cart.
     * @param {string} attendeeId - The ID of the attendee.
     * @param {string} ticketID - The ID of the ticket.
     * @param {string} eventKey - The key of the event.
     * @param {string} sourceCode - The source code of the ticket.
    */
    async insertTicketToCart(attendeeId, ticketID, eventKey, sourceCode) {
        try {
            // If no ticketID provided, get delegate ticket
            if (!ticketID && eventKey) {
                const delegateTicket = await strapi.db.connection.context.raw(
                    `SELECT "SFID" FROM events."EventApi__Ticket_Type__c" 
                     WHERE "Ticket_Type__c" = ? AND "EventApi__Event__c" = ? LIMIT 1`,
                    ['Delegate', eventKey]
                );

                if (delegateTicket?.rows?.[0]) {
                    ticketID = delegateTicket.rows[0].SFID;
                }
            }

            if (!ticketID) {
                console.warn('No ticket ID available');
                return [];
            }

            // Check if already in cart
            const existingCart = await strapi.db.connection.context.raw(
                `SELECT id FROM events."EventApi__Cart__c" 
                 WHERE ticket_id = ? AND attendee_id = ? LIMIT 1`,
                [ticketID, attendeeId]
            );

            if (existingCart?.rows?.length) {
                console.warn('Ticket already in cart');
                return [];
            }

            // Insert new cart entry
            const result = await strapi.db.connection.context.raw(
                `INSERT INTO events."EventApi__Cart__c"(ticket_id, attendee_id, source_code)
                 VALUES (?, ?, ?) RETURNING *`,
                [ticketID, attendeeId, sourceCode]
            );

            return result?.rows || [];
        } catch (error) {
            console.error('Error in insertTicketToCart:', error);
            throw new ApplicationError("Failed to insert ticket to cart", error);
        }
    },
    /**
     * Inserts sub event attendees into the cart.
     * @param {Object} request - The request object containing attendee information.
     * @returns {Promise<Array>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async insertSubEventAttendee(request) {
        let result = [];

        if (request.attID) {
            for (const [index, attID] of request.attID.entries()) {
                const insertTicketToCart = await this.insertTicketToCart(attID, request.ticketID, '', '');
                if (insertTicketToCart && insertTicketToCart.rows && insertTicketToCart.rows.length > 0) {
                    result.push(insertTicketToCart.rows[0]);
                }
            }
        }

        return result
    },
    /**
     * Inserts a single attendee into the database.
     * @param {Object} request - The request object containing attendee information.
     * @returns {Promise<Object>} The result of the operation.
     * @throws {ApplicationError} If there is an error in the operation.
     */
    async insertSingleAttendee(request) {
        let result;

        if (request.eventKey == null || request.eventKey == "") {
            throw new ApplicationError("Err: No Event Key");
        }

        if (request.primaryContact == null || request.primaryContact == "") {
            throw new ApplicationError("Err: No Primary Attendee");
        }
        //Upsert Accompany Contact
        if (request.guests !== null && request.guests !== "") {
            const guestsEntries = Object.entries(request.guests);
            for (const [index, guest] of guestsEntries) {
                let guestId = await this.upsertContactSF(guest, request.accessToken);
                if (guestId && guestId.length > 0) {
                    request.guests[index].sfid = guestId;
                    let upsertContactFormData_g = await this.upsertContactFormData(request.eventKey, request.guests[index], request.primaryContact.attId);
                    if (upsertContactFormData_g.length > 0) {
                        request.guests[index].attId = upsertContactFormData_g[0].id
                        const insertTicketToCart = await this.insertTicketToCart(upsertContactFormData_g[0].id, request.ticketID, request.eventKey, '');
                    }
                }
            };
        }
        //Upsert Accompany Contact

        return request;
    },
    /**
     * Gets order summary for attendees with improved performance.
     * @param {Object} request - Request containing primary contact ID.
     * @returns {Array} - Array of order summary records.
     * @throws {ApplicationError} - If retrieval fails.
     */
    async getOrderSummary(request) {
        try {
            // Get all data in a single optimized query
            const query = `
                WITH attendee_tickets AS (
                    SELECT 
                        fd.*,
                        json_agg(
                            json_build_object(
                                'id', c.id,
                                'ticket_id', c.ticket_id,
                                'source_code', c.source_code,
                                'ticket_name', tt."Name",
                                'price_rules', COALESCE((
                                    SELECT json_agg(
                                        json_build_object(
                                            'item', pr."OrderApi__Item__c",
                                            'source_code', pr."OrderApi__Required_Source_Codes__c",
                                            'id', pr."SFID",
                                            'name', pr."Name",
                                            'price', pr."OrderApi__Price__c",
                                            'priceWithTax', pr."OrderApi__Tax_Inclusive_Price__c",
                                            'currency', pr."CurrencyIsoCode",
                                            'additional_currencies', pr."OrderApi__Additional_Currencies_JSON__c",
                                            'max_available', pr."OrderApi__Max_Num_Available__c",
                                            'rule', (
                                                SELECT json_agg(
                                                    json_build_object(
                                                        'field', prv."OrderApi__Field__c",
                                                        'operator', prv."OrderApi__Operator__c",
                                                        'value', prv."OrderApi__Value__c",
                                                        'OrderApi__Field__c', prv."OrderApi__Field__c",
                                                        'OrderApi__Operator__c', prv."OrderApi__Operator__c",
                                                        'OrderApi__Value__c', prv."OrderApi__Value__c"
                                                    )
                                                )
                                                FROM events."OrderApi__Price_Rule_Variable__c" prv
                                                WHERE prv."OrderApi__Price_Rule__c" = pr."SFID"
                                            )
                                        )
                                    )
                                    FROM events."OrderApi__Price_Rule__c" pr
                                    WHERE pr."EventApi__Ticket_Type__c" = tt."SFID"
                                    AND pr."OrderApi__Is_Active__c" = true
                                    AND (
                                        (c.source_code IS NOT NULL AND c.source_code <> '' AND (pr."OrderApi__Required_Source_Codes__c" = c.source_code OR pr."OrderApi__Required_Source_Codes__c" IS NULL))
										  OR
										  (COALESCE(c.source_code, '') = '' AND pr."OrderApi__Required_Source_Codes__c" IS NULL)
                                    )
                                    AND CURRENT_DATE BETWEEN pr."OrderApi__Start_Date__c" AND pr."OrderApi__End_Date__c"
                                    AND pr."Name" <> 'DEFAULT'
                                ), (
                                    SELECT json_agg(
                                        json_build_object(
                                            'item', pr."OrderApi__Item__c",
                                            'source_code', pr."OrderApi__Required_Source_Codes__c",
                                            'id', pr."SFID",
                                            'name', pr."Name",
                                            'price', pr."OrderApi__Price__c",
                                            'priceWithTax', pr."OrderApi__Tax_Inclusive_Price__c",
                                            'currency', pr."CurrencyIsoCode",
                                            'additional_currencies', pr."OrderApi__Additional_Currencies_JSON__c",
                                            'max_available', pr."OrderApi__Max_Num_Available__c",
                                            'rule', (
                                                SELECT json_agg(
                                                    json_build_object(
                                                        'field', prv."OrderApi__Field__c",
                                                        'operator', prv."OrderApi__Operator__c",
                                                        'value', prv."OrderApi__Value__c",
                                                        'OrderApi__Field__c', prv."OrderApi__Field__c",
                                                        'OrderApi__Operator__c', prv."OrderApi__Operator__c",
                                                        'OrderApi__Value__c', prv."OrderApi__Value__c"
                                                    )
                                                )
                                                FROM events."OrderApi__Price_Rule_Variable__c" prv
                                                WHERE prv."OrderApi__Price_Rule__c" = pr."SFID"
                                            )
                                        )
                                    )
                                    FROM events."OrderApi__Price_Rule__c" pr
                                    WHERE pr."EventApi__Ticket_Type__c" = tt."SFID"
                                    AND pr."OrderApi__Is_Active__c" = true
                                    AND pr."Name" = 'DEFAULT'
                                ))
                            )
                        ) FILTER (WHERE c.id IS NOT NULL) as ticket_type
                    FROM events."EventApi__FormData__c" fd
                    LEFT JOIN events."EventApi__Cart__c" c ON c.attendee_id::text = fd.id::text
                    LEFT JOIN events."EventApi__Ticket_Type__c" tt ON tt."SFID" = c.ticket_id
                    WHERE (fd.id = ? OR fd.primary_attendee_id::text = ?)
                    GROUP BY fd.id
                )
                SELECT * FROM attendee_tickets
                ORDER BY id;
            `;

            const result = await strapi.db.connection.context.raw(query, [
                request.primaryContactId,
                request.primaryContactId
            ]);

            if (!result.rows.length) {
                return [];
            }

            // Process results and apply condition checking
            const processedResults = await Promise.all(result.rows.map(async row => {

                if (!row.ticket_type) {
                    return row;
                }


                // Process each ticket type
                row.ticket_type = await Promise.all(row.ticket_type.map(async ticket => {
                    if (!ticket.price_rules) {
                        return {
                            ...ticket,
                            tickets: null
                        };
                    }


                    // Process pricing rules using the dedicated function
                    const eligibleRules = await processPricingRules(ticket.price_rules, {
                        id: row.id,
                        form_data: row.form_data
                    });
console.log("elegible rules",eligibleRules)


                    if (!eligibleRules.length) {
                        const defaultRule = ticket.price_rules.find(rule => rule.name === 'DEFAULT');

                        if (defaultRule) {
                            const tickets = {
                                id: defaultRule?.id || '00',
                                name: defaultRule?.name || 'DEFAULT',
                                price: defaultRule?.additional_currencies?.[0]?.price ?? defaultRule?.price,
                                priceWithTax: defaultRule?.priceWithTax,
                                currency: defaultRule?.additional_currencies?.[0]?.currencyType ?? defaultRule?.currency,
                                max_available: defaultRule?.max_available,
                                item: defaultRule?.item
                            };

                            return {
                                ...ticket,
                                tickets
                            };
                        }
                        else {
                            return {
                                ...ticket,
                                tickets: null
                            }
                        }

                    }

                    const oneDayDates = Array.isArray(row?.form_data?.one_day_delegate_date__c)
                    ? row.form_data.one_day_delegate_date__c
                    : [];
                
                const hasOneDayDates = oneDayDates.length > 0;
                
                if (!eligibleRules.length) {
                    const defaultRule = ticket.price_rules.find(rule => rule.name === 'DEFAULT');
                
                    if (defaultRule) {
                        const tickets = {
                            id: defaultRule?.id || '00',
                            name: defaultRule?.name || 'DEFAULT',
                            price: defaultRule?.additional_currencies?.[0]?.price ?? defaultRule?.price,
                            priceWithTax: defaultRule?.priceWithTax,
                            currency: defaultRule?.additional_currencies?.[0]?.currencyType ?? defaultRule?.currency,
                            max_available: defaultRule?.max_available,
                            item: defaultRule?.item
                        };
                
                        return {
                            ...ticket,
                            tickets
                        };
                    } else {
                        return {
                            ...ticket,
                            tickets: null
                        };
                    }
                }
                
                row['eligibleRules'] = eligibleRules;
                
                // if one day dates available => return all matched rules
                if (hasOneDayDates) {
                    const tickets = eligibleRules.map(rule => ({
                        id: rule.id,
                        name: rule.name,
                        price: rule.additional_currencies?.[0]?.price ?? rule.price,
                        priceWithTax: rule.priceWithTax,
                        currency: rule.additional_currencies?.[0]?.currencyType ?? rule.currency,
                        max_available: rule.max_available,
                        item: rule.item,
                        source_code: rule.source_code
                    }));
                console.log("tickets",tickets)
                    return {
                        ...ticket,
                        tickets
                    };
                }
                
                // otherwise keep old behavior => choose best one
                eligibleRules.sort((a, b) => (b.rule?.length || 0) - (a.rule?.length || 0));
                
                const withSource = eligibleRules.filter(rule => rule.source_code != null);
                const bestRule = withSource.length > 0 ? withSource[0] : eligibleRules[0];
                
                const tickets = {
                    id: bestRule.id,
                    name: bestRule.name,
                    price: bestRule.additional_currencies?.[0]?.price ?? bestRule.price,
                    priceWithTax: bestRule.priceWithTax,
                    currency: bestRule.additional_currencies?.[0]?.currencyType ?? bestRule.currency,
                    max_available: bestRule.max_available,
                    item: bestRule.item
                };
                
                return {
                    ...ticket,
                    tickets
                };
                }));

                return row;
            }));

            return processedResults;

        } catch (error) {
            console.error('Error in getOrderSummary:', error);
            throw new ApplicationError("Failed to get order summary", error);
        }
    },

    async checkAttendeeEmail(emails, eventKey, accessToken) {
        let result = [];
        const str = emails.map(item => `'${item}'`).join(", ");
        try {
            var conn = await this.getJSforceConnection(accessToken);
            const attendee = await conn.query(`SELECT EventApi__Preferred_Email__c, EventApi__Attendee_Event__c 
                                        FROM EventApi__Attendee__c  
                                        WHERE EventApi__Preferred_Email__c IN (${str}) 
                                        AND EventApi__Attendee_Event__c = '${eventKey}'`)
                .on("record", async function (attendee) {
                    result.push(attendee);
                })

        } catch (error) {
            console.error('Error in checkAttendeeEmail:', error);
        }
        return result;
    },
    async deleteTicket(request) {
        let result;
        try {
            let qry = `DELETE FROM events."EventApi__Cart__c" where id = ?`;
            result = await strapi.db.connection.context.raw(qry, [request]);
        } catch (error) {
            throw new ApplicationError("Err:", error);
        }
        return result.rows[0];
    },
    async applyDiscountCode(request) {
        let result = [];

        try {
            let qry = `SELECT t1."SFID" as DSFID, t1."Name" as DName, t2.* FROM events."OrderApi__Source_Code__c" as t1
                    join events."OrderApi__Price_Rule__c" as t2 on t1."SFID" = any (string_to_array(t2."OrderApi__Required_Source_Codes__c", ',')) 
                    where t1."Name" = ?`;
            let checkSourceCode = await strapi.db.connection.context.raw(qry, [request?.code]);
            if (checkSourceCode.rows.length > 0) {
                if (request.attendee !== null && request.attendee.length > 0) {
                    for (const att of request.attendee) {
                        let updateQry = `UPDATE events."EventApi__Cart__c" SET source_code=? WHERE attendee_id=? returning *`;
                        let updateQry_data = await strapi.db.connection.context.raw(updateQry, [checkSourceCode.rows[0]?.dsfid, att]);
                        result.push(updateQry_data.rows)
                    }
                }
            }
            else {
                result.push({ err: 'Invalid Source Code' })
            }
        } catch (error) {
            throw new ApplicationError("Err: ", error);
        }

        return result
    },
    /** Attendee - Page - Overview   */

    // ============================================================================
    // Private Helper Methods
    // ============================================================================

    /**
     * Cleans up expired cache entries
     * @private
     * @param {Map} cache - Cache Map object to clean
     * @param {number} [ttl=CACHE_TTL.SHORT] - Time to live in milliseconds
     */
    _cleanupCache(cache, ttl = CACHE_TTL.SHORT) {
        if (cache.size > CACHE_LIMITS.MAX_SIZE) {
            const now = Date.now();
            for (const [key, value] of cache.entries()) {
                if (now - value.timestamp > ttl) {
                    cache.delete(key);
                }
            }
        }
    },

    /**
     * Validates and sanitizes input parameters
     * @private
     * @param {Object} params - Parameters to validate
     * @param {Array<string>} required - List of required parameter names
     * @throws {ApplicationError} If validation fails
     */
    _validateParams(params, required) {
        const missing = required.filter(param => !params[param]);
        if (missing.length > 0) {
            throw new ApplicationError(`Missing required parameters: ${missing.join(', ')}`);
        }
    }
});

// ============================================================================
// Helper Functions Section
// ============================================================================

/**
 * Processes pricing rules by checking conditions and available quotas.
 * Uses caching to improve performance.
 * 
 * @param {Array} pricingRules - Array of pricing rules to process
 * @param {Object} ctx - Context object containing form data and ID
 * @returns {Promise<Array>} Array of eligible pricing rules
 */
const processPricingRules = async (pricingRules, ctx) => {
    try {
        if (!Array.isArray(pricingRules) || !pricingRules.length) {
            return [];
        }

        // Process rules in parallel for better performance
        const results = await Promise.all(pricingRules.map(async (rule) => {
            try {
                // Skip invalid rules early
                if (!rule || !rule.rule) {
                    return null;
                }

                const conditions = generateSQLConditions(rule.rule);
                const [isEligible, availableSpots] = await Promise.all([
                    checkConditionMatch(conditions, ctx.id),
                    rule.OrderApi__Max_Num_Available__c > 0 ? checkAvailableSpots(rule.SFID) : null
                ]);

                if (!isEligible) {
                    return null;
                }

                const maxAvailable = rule.OrderApi__Max_Num_Available__c;
                if (maxAvailable > 0 && availableSpots >= maxAvailable) {
                    return null;
                }
                return rule;
            } catch (error) {
                console.error(`Error processing rule ${rule?.SFID}:`, error);
                return null;
            }
        }));

        // Filter out null results and return valid rules
        return results.filter(Boolean);
    } catch (error) {
        console.error('Error in processPricingRules:', error);
        throw new ApplicationError('Failed to process pricing rules', error);
    }
};

/**
 * Generates optimized SQL condition string from rule set.
 * @param {Array} rules - Array of rule conditions
 * @returns {string} - SQL conditions string
 */
const generateSQLConditions = (rules) => {
    if (!Array.isArray(rules) || !rules.length) {
        return '';
    }

    const validOperators = {
        'not equal to': '<>',
        'equals': '=',
        'greater than': '>',
        'less than': '<',
        'greater than or equal to': '>=',
        'less than or equal to': '<='
    };

    // Use a Set to deduplicate conditions
    const conditions = new Set();

    // rules.forEach(rule => {
    //     if (!rule?.OrderApi__Field__c || !rule?.OrderApi__Value__c) {
    //         return;
    //     }

    //     const operator = validOperators[rule.OrderApi__Operator__c] || '=';
    //     // Escape field names and values to prevent SQL injection
    //     const field = rule.OrderApi__Field__c.replace(/[^a-zA-Z0-9_]/g, '');
    //     const value = rule.OrderApi__Value__c.replace(/'/g, "''");

    //     conditions.add(`AND form_data->>'${field}' ${operator} '${value}'`);
    // });
    rules.forEach(rule => {
        if (!rule?.OrderApi__Field__c || !rule?.OrderApi__Value__c) {
            return;
        }
    
        const operator = validOperators[rule.OrderApi__Operator__c] || '=';
        const field = rule.OrderApi__Field__c.replace(/[^a-zA-Z0-9_]/g, '');
        const value = String(rule.OrderApi__Value__c).replace(/'/g, "''");
    
        if (field === 'is_one_day_registration__c') {
            conditions.add(
                `AND LOWER(form_data->>'${field}') ${operator} LOWER('${value}')`
            );
        } else if (field === 'one_day_delegate_date__c') {
            const normalizedValue = value.replace(/\//g, '-');
    
            if (operator === '=') {
                conditions.add(`
                    AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(
                            CASE
                                WHEN jsonb_typeof((form_data->'${field}')::jsonb) = 'array'
                                THEN (form_data->'${field}')::jsonb
                                ELSE '[]'::jsonb
                            END
                        ) AS dt(val)
                        WHERE REPLACE(dt.val, '/', '-') = '${normalizedValue}'
                    )
                `);
            } else if (operator === '<>') {
                conditions.add(`
                    AND NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(
                            CASE
                                WHEN jsonb_typeof((form_data->'${field}')::jsonb) = 'array'
                                THEN (form_data->'${field}')::jsonb
                                ELSE '[]'::jsonb
                            END
                        ) AS dt(val)
                        WHERE REPLACE(dt.val, '/', '-') = '${normalizedValue}'
                    )
                `);
            }
        } else {
            conditions.add(
                `AND form_data->>'${field}' ${operator} '${value}'`
            );
        }
    });
    return Array.from(conditions).join(' ');
};

/**
 * Checks if any condition matches in form data with caching.
 * @param {string} conditions - SQL conditions string
 * @param {string} id - Form data ID
 * @returns {Promise<boolean>} - Whether conditions match
 */
const checkConditionMatch = (() => {
    const cache = new Map();
    const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

    return async (conditions, id) => {
        try {
            // const cacheKey = `${id}:${conditions}`;
            // const cached = cache.get(cacheKey);

            // if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
            //     return cached.result;
            // }

            const query = `
                SELECT 1 
                FROM events."EventApi__FormData__c" 
                WHERE id = ? ${conditions} 
                LIMIT 1
            `;


            const result = await strapi.db.connection.raw(query, [id]);
            const matches = result.rows.length > 0;

            // Cache the result
            // cache.set(cacheKey, {
            //     result: matches,
            //     timestamp: Date.now()
            // });

            // // Clean up old cache entries
            // if (cache.size > 1000) {
            //     const now = Date.now();
            //     for (const [key, value] of cache.entries()) {
            //         if (now - value.timestamp > CACHE_TTL) {
            //             cache.delete(key);
            //         }
            //     }
            // }

            return matches;
        } catch (error) {
            console.error('Error in checkConditionMatch:', error);
            return false;
        }
    };
})();

/**
 * Checks the number of spots used for a pricing rule with caching.
 * @param {string} ruleSFID - The Salesforce ID of the pricing rule
 * @returns {Promise<number>} - The number of used spots
 */
const checkAvailableSpots = (() => {
    const cache = new Map();
    const CACHE_TTL = 60 * 1000; // 1 minute

    return async (ruleSFID) => {
        try {
            const cached = cache.get(ruleSFID);
            if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
                return cached.count;
            }

            const query = `
                SELECT COUNT(t2.id) AS used_spots
                FROM events."EventApi__Sales_Order__c" t1
                JOIN events."EventApi__Sales_Order_Line__c" t2 
                    ON t1.id = t2."OrderApi__Sales_Order__c"
                WHERE t1."OrderApi__Sales_Order_Status__c" = 'Paid' 
                    AND t2."OrderApi__Price_Rule__c" = ?
            `;

            const result = await strapi.db.connection.raw(query, [ruleSFID]);
            const count = parseInt(result.rows[0]?.used_spots || 0, 10);

            // Cache the result
            cache.set(ruleSFID, {
                count,
                timestamp: Date.now()
            });

            return count;
        } catch (error) {
            console.error('Error in checkAvailableSpots:', error);
            return 0;
        }
    };
})();
