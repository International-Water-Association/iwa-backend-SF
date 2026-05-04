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

module.exports = () => ({
    /**
     * Establishes a connection to Salesforce using jsforce.
     * @param {string} request - The access token for Salesforce authentication
     * @returns {Promise<Object>} The jsforce connection object
     * @throws {ApplicationError} If connection fails
     */
  

    
   

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
    

    /**
     * Inserts or updates a contact form data record in the database.
     * @param {string} event_key - The event key.
     * @param {Object} data - The data object containing contact information.
     * @param {string} pa - The primary attendee ID.
     * @returns {Promise<Array>} The result of the operation.
    */
    
    /**
     * Inserts a ticket into the cart.
     * @param {string} attendeeId - The ID of the attendee.
     * @param {string} ticketID - The ID of the ticket.
     * @param {string} eventKey - The key of the event.
     * @param {string} sourceCode - The source code of the ticket.
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
