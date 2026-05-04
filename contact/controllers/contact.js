'use strict';

/**
 *  contact controller
 */


const axios = require("axios");
const { createCoreController } = require('@strapi/strapi').factories;

module.exports = createCoreController('api::contact.contact', {

  async getToken(ctx) {
    const request = ctx.params;
    let result = [];

    try {
      result = await axios.post(process.env.SF_LOGIN);
    } catch (error) {
      result = { Msg: "No Result", data: error };
    }
    return result.data;

  },

  
  

  /** Renewal Window */
  async getAllSubscription(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select id, OrderApi__Activated_Date__c, OrderApi__Paid_Through_Date__c, OrderApi__Status__c, OrderApi__Item__c, OrderApi__Item__r.Name, OrderApi__Item__r.FON_Journal_Item__c, OrderApi__Subscription_Plan__c, OrderApi__Subscription_Plan__r.Name, OrderApi__Subscription_Plan__r.OrderApi__PreTerm_Renewal_Window__c, OrderApi__Subscription_Plan__r.OrderApi__PostTerm_Renewal_Window__c, FON_Membership_Number__c, OrderApi__Grace_Period_End_Date__c, OrderApi__In_Grace_Period__c, OrderApi__Term_Start_Date__c, OrderApi__Term_End_Date__c, OrderApi__Days_To_Lapse__c, OrderApi__Sales_Order_Line__r.OrderApi__Sales_Order__r.FON_Invoice_Link__c from OrderApi__Subscription__c where OrderApi__Contact__c = '${request.id}' order by OrderApi__Status__c asc`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  async getOneSubscription(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select id, OrderApi__Status__c, OrderApi__Item__c, OrderApi__Item__r.Name, OrderApi__Subscription_Plan__c, OrderApi__Subscription_Plan__r.Name, OrderApi__Subscription_Plan__r.Id, FON_Membership_Number__c, OrderApi__Grace_Period_End_Date__c, OrderApi__In_Grace_Period__c, OrderApi__Term_Start_Date__c, OrderApi__Term_End_Date__c, OrderApi__Days_To_Lapse__c from OrderApi__Subscription__c where Id = '${request.id}'`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  async getAllorders(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select Id, Name, OrderApi__Date__c, New_Receipt_URL__c, OrderApi__Sales_Order__r.Event__c from OrderApi__Receipt__c where OrderApi__Contact__c = '${request.id}' order by OrderApi__Date__c desc`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  async getTerms(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select id, Name, OrderApi__Renewed_Date__c,  OrderApi__Term_Start_Date__c, OrderApi__Term_End_Date__c, OrderApi__Grace_Period_End_Date__c, OrderApi__Is_Active__c, OrderApi__Sales_Order__r.Name, OrderApi__Sales_Order__r.FON_Invoice_Link__c from OrderApi__Renewal__c where OrderApi__Subscription__c = '${request.id}' order by name desc`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  async getRenewPath(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select Id, OrderApi__Renew_Into_Item__r.ID, OrderApi__Renew_Into_Item__r.Name from OrderApi__Renewal_Path__c where OrderApi__Item__c = '${request.id}'`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  async getSPlans(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select Id, Name, OrderApi__Is_Active__c, Is_Test_Data__c, OrderApi__Disable_Renew__c from OrderApi__Subscription_Plan__c where OrderApi__Type__c = 'Termed' and OrderApi__Is_Active__c = true`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  async getAllJournal(ctx) {
    const request = ctx.request.body;
    let result;

    try{
      let token = await AuthSF({});

      const BaseURL = process.env.SF_BASEURL + "/services/data/v36.0/query/?q=";


      let event_s = await axios({
        method: "get",
        url:
          BaseURL +
          `select Id, Name, OrderApi__Image_Path__c from OrderApi__Item__c where OrderApi__Item_Class__c = 'a134K000000P4dGQAS' and OrderApi__Is_Active__c = true`,
        headers: { Authorization: `Bearer ${token.access_token}` },
      });

      result = event_s.data
    }
    catch(e){
      console.log(e)
      result = {}
    }

    return { result };
  },
  /** Renewal Window */

  
  
});

const AuthSF = async (data)=>{
  try {
    var token = await axios.post(
      process.env.SF_LOGIN
    );
  } catch (error) {
    var token = { Msg: "No Result", error: error };
  }
  return token.data;
};
