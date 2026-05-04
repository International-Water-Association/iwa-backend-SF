'use strict';

/**
 * @fileoverview Event routes configuration file
 * Defines all API endpoints related to event management and attendee operations
 */

module.exports = {
  routes: [
    // ============================================================================
    // Admin Routes
    // ============================================================================
    {
      method: 'GET',
      path: '/event-admin/syncDB/:key',
      handler: 'event-management-system.syncEventDB',
      config: {
        description: 'Synchronize event database with Salesforce',
      }
    },

    // ============================================================================
    // Attendee Management Routes
    // ============================================================================
    
    // Event Information Endpoints
    {
      method: 'GET',
      path: '/event-attendee/get-all-events',
      handler: 'event-management-system.getAllEvents',
      config: {
        description: 'Retrieve all active events'
      }
    },
    {
      method: 'GET',
      path: '/event-attendee/get-basic-event-data/:key',
      handler: 'event-management-system.getBasicEventData',
      config: {
        description: 'Get basic information for a specific event'
      }
    },
    {
      method: 'GET',
      path: '/event-attendee/get-venue-data/:key',
      handler: 'event-management-system.getVenueData',
      config: {
        description: 'Get venue information for a specific event'
      }
    },

    // Attendee Registration Endpoints
    {
      method: 'POST',
      path: '/event-attendee/validate-email',
      handler: 'event-management-system.validateEmail',
      config: {
        description: 'Validate attendee email address'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/send-email',
      handler: 'event-management-system.sendEmail',
      config: {
        description: 'Send confirmation email to attendee'
      }
    },

    {
      method: 'POST',
      path: '/event/activity-log',
      handler: 'event-management-system.activityLog',
      config: {
        description: 'create activity log'
      }
    },

    // Reference Data Endpoints
    {
      method: 'GET',
      path: '/event-attendee/get-country',
      handler: 'event-management-system.getAllCountry',
      config: {
        description: 'Get list of available countries'
      }
    },
    {
      method: 'GET',
      path: '/event-attendee/get-guest-token',
      handler: 'event-management-system.getEventGuestRegToken',
      config: {
        description: 'Get registration token for guest attendees'
      }
    },

    // Sub-Event Management Endpoints
    {
      method: 'GET',
      path: '/event-attendee/get-sub-event/:key',
      handler: 'event-management-system.getSubEvent',
      config: {
        description: 'Get sub-events for a specific event'
      }
    },

    // Attendee Registration Endpoints
    {
      method: 'POST',
      path: '/event-attendee/insert-attendee',
      handler: 'event-management-system.insertAttendee',
      config: {
        description: 'Register new attendee with guests'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/insert-sub-event-attendee',
      handler: 'event-management-system.insertSubEventAttendee',
      config: {
        description: 'Register attendee for sub-events'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/insert-single-attendee',
      handler: 'event-management-system.insertSingleAttendee',
      config: {
        description: 'Register single attendee'
      }
    },

    // Order Management Endpoints
    {
      method: 'POST',
      path: '/event-attendee/get-order-summary',
      handler: 'event-management-system.getOrderSummary',
      config: {
        description: 'Get order summary for attendees'
      }
    },
    {
      method: 'DELETE',
      path: '/event-attendee/delete-ticket/:key',
      handler: 'event-management-system.deleteTicket',
      config: {
        description: 'Delete ticket from cart'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/apply-discount-code',
      handler: 'event-management-system.applyDiscountCode',
      config: {
        description: 'Apply discount code to order'
      }
    },
    {
      method: 'GET',
      path: '/event-attendee/get-form-data/:id',
      handler: 'event-management-system.getFormData',
      config: {
        description: 'Get form data for an attendee'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/update-form-data',
      handler: 'event-management-system.updateFormData',
      config: {
        description: 'Update form data for an attendee'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/prepare-additional-ticket',
      handler: 'event-management-system.prepareAdditionalTicket',
      config: {
        description: 'Prepare additional ticket for an attendee'
      }
    },
    {
      method: 'POST',
      path: '/event-attendee/get-attendee-by-sales-order-id',
      handler: 'event-management-system.getAttendeeBySalesOrderId',
      config: {
        description: 'Get attendee by sales order id'
      }
    }
  ]
};
