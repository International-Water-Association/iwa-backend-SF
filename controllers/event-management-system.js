'use strict';

/**
 * @fileoverview Event controller module
 * Contains action handlers for all event-related API endpoints
 * 
 * @module events-controller
 * @requires node-ses
 * @requires axios
 */

const ses = require("node-ses");
const axios = require("axios");

// Initialize SES client for email sending
const client = ses.createClient({ key: process.env.AWS_ACCESS_KEY_ID, secret: process.env.AWS_SECRET_ACCESS_KEY });

module.exports = {
  /**
   * Synchronizes the event database with Salesforce
   * @param {Object} ctx - Koa context object
   * @param {Function} next - Koa next middleware function
   * @returns {Promise<Object>} Success status or error message
   */
  syncEventDB: async (ctx, next) => {
    ctx.req.setTimeout(0);

    ctx.respond = false;
    ctx.status = 200;

    ctx.set('Content-Type', 'text/event-stream');
    ctx.set('Cache-Control', 'no-cache');
    ctx.set('Connection', 'keep-alive');

    const sendEvent = (status, message) => {
      ctx.res.write(`data: ${JSON.stringify({ status, message })}\n\n`);
    };

    try {
      const { key } = ctx.params;

      // Basic guard to ensure a key was provided before performing the sync.
      if (!key) {
        sendEvent('error', 'Missing required parameter: key');
        return ctx.res.end();
      }

      sendEvent('info', 'Starting events sync...');

      // events data.
      await strapi
        .service('api::event-management-system.event-management-system')
        .syncEventsDB(key, sendEvent);

      ctx.res.end();

    } catch (error) {
      strapi.log.error('syncEventsDB Error:', error);
      sendEvent('error', `Sync failed: ${error.message}`);
    } 
  },

  // ============================================================================
  // Event Information Controllers
  // ============================================================================

  /**
   * Retrieves all active events
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} List of events
   */
  getAllEvents: async (ctx) => {
    try {
      const response = await strapi
        .service("api::event-management-system.event-management-system")
        .getAllEvents();

      ctx.body = { status: 'success', data: response };
    } catch (err) {
      console.error('Error in getAllEvents:', err);
      return ctx.badRequest("Failed to retrieve events", err);
    }
  },

  /**
   * Gets basic event data by key
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} Event data
   */
  getBasicEventData: async (ctx) => {
    try {
      const request = ctx.params;
      const response = await strapi
        .service("api::event-management-system.event-management-system")
        .getBasicEventData(request.key);

      ctx.body = { status: 'success', data: response };
    } catch (err) {
      console.error('Error in getBasicEventData:', err);
      return ctx.badRequest("Failed to retrieve event data", err);
    }
  },

  /**
   * Gets venue data for an event
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} Venue data
   */
  getVenueData: async (ctx) => {
    try {
      const request = ctx.params;
      const response = await strapi
        .service("api::event-management-system.event-management-system")
        .getVenueData(request.key);

      ctx.body = { status: 'success', data: response };
    } catch (err) {
      console.error('Error in getVenueData:', err);
      return ctx.badRequest("Failed to retrieve venue data", err);
    }
  },

  // ============================================================================
  // Attendee Management Controllers
  // ============================================================================

  /**
   * Validates attendee email address
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} Validation result
   */
  validateEmail: async (ctx) => {
    try {
      const request = ctx.request.body;
      const result = await strapi
        .service("api::event-management-system.event-management-system")
        .validateEmail(request?.email);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.error('Error in validateEmail:', err);
      return ctx.badRequest("Email validation failed", err);
    }
  },

  /**
   * Sends email to attendee
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} Email sending result
   */
  sendEmail: async (ctx) => {
    try {
      const request = ctx.request.body;

      // Send email using SES
      await client.sendEmail({
        to: request.email,
        from: "International Water Association - Event <no-reply@iwahq.org>",
        replyTo: request.to || "registration.wwce@iwahq.org",
        subject: request.subject,
        message: Buffer.from(request.cdx, 'base64').toString('utf-8')
      }, function (err, data, res) {
        console.log(err);
      });

      ctx.body = { status: 'success', data: {} };
    } catch (err) {
      console.error('Error in sendEmail:', err);
      return ctx.badRequest("Failed to send email", err);
    }
  },

  /**
   * Gets list of all countries
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} List of countries
   */
  getAllCountry: async (ctx) => {
    try {
      const response = await strapi
        .service("api::event-management-system.event-management-system")
        .getAllCountry();

      ctx.body = { status: 'success', data: response };
    } catch (err) {
      console.error('Error in getAllCountry:', err);
      return ctx.badRequest("Failed to retrieve country list", err);
    }
  },

  /**
   * Gets guest registration token
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} Registration token
   */
  getEventGuestRegToken: async (ctx) => {
    try {
      const response = await axios.post(process.env.SF_LOGIN);
      ctx.body = { status: 'success', data: response.data.access_token };
    } catch (err) {
      console.error('Error in getEventGuestRegToken:', err);
      return ctx.badRequest("Failed to get registration token", err);
    }
  },

  getSubEvent: async (ctx, next) => {
    try {
      const request = ctx.params;

      const response = await strapi.service("api::event-management-system.event-management-system").getSubEvent(request.key);

      // const groupedData = response.reduce((acc, item) => {
      //   if (!acc[item.Ticket_Type__c]) {
      //     acc[item.Ticket_Type__c] = [];
      //   }
      //   acc[item.Ticket_Type__c].push(item);
      //   return acc;
      // }, {});

      // const separatedJSON = Object.entries(groupedData).map(([Ticket_Type__c, items]) => ({
      //   Ticket_Type__c,
      //   data: items
      // })).sort((a, b) => a.Ticket_Type__c.localeCompare(b.Ticket_Type__c));

      ctx.body = { status: 'success', data: response };
    } catch (err) {
      return ctx.badRequest("Something went wrong", err);
    }
  },
  insertAttendee: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .insertAttendee(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  insertSubEventAttendee: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .insertSubEventAttendee(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  insertSingleAttendee: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .insertSingleAttendee(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  getOrderSummary: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .getOrderSummary(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  deleteTicket: async (ctx, next) => {
    try {
      const request = ctx.params;

      const response = await strapi.service("api::event-management-system.event-management-system").deleteTicket(request.key);

      ctx.body = { status: 'success', data: response };
    } catch (err) {
      return ctx.badRequest("Something went wrong", err);
    }
  },
  applyDiscountCode: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .applyDiscountCode(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  activityLog: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi.service("api::event-management-system.event-management-system").activityLog(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },

  getFormData: async (ctx, next) => {
    try {
      const request = ctx.params;
      let result = await strapi.service("api::event-management-system.event-management-system").getFormData(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },

  updateFormData: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .updateFormData(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  prepareAdditionalTicket: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi
        .service("api::event-management-system.event-management-system")
        .prepareAdditionalTicket(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },

  /**
   * Gets attendee by sales order id
   * @param {Object} ctx - Koa context
   * @returns {Promise<Object>} Attendee data
   */
  getAttendeeBySalesOrderId: async (ctx, next) => {
    try {
      const request = ctx.request.body;
      let result = await strapi.service("api::event-management-system.event-management-system").getAttendeeBySalesOrderId(request);

      ctx.body = { status: 'success', data: result };
    } catch (err) {
      console.log(err);

      return ctx.badRequest("Something went wrong", err);
    }
  },
  /** Attendee - Page - Overview   */
};
