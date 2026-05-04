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
  


};
