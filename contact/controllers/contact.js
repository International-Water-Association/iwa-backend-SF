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

  async getFeaturedByWeek(ctx) {
    const id = ctx.params;
    let result;

    let MainQuery =
      "SELECT * FROM salesforce.contact where iwa_week_number__c = ? ORDER BY fm_slot__c asc LIMIT 3";

    try {
      const data = await strapi.db.connection.context.raw(MainQuery, [id.id]);

      result = data.rows;
    } catch (error) {
      result = { Msg: "No Result", error: '' };
    }

    return { result };
  },
  async getSuggFM(ctx) {
    const id = ctx.params;
    let result;

    let MainQuery = `select t2.* from salesforce."View_FM_Slot1" as t1 join salesforce."View_FM_Basic" as t2 on t1.sfid = t2.sfid order by days_to_expiry__c desc Limit 150`;
    let MainQuery2 = `select t2.* from salesforce."View_FM_Slot2" as t1 join salesforce."View_FM_Basic" as t2 on t1.sfid = t2.sfid order by days_to_expiry__c desc Limit 150`;
    let MainQuery3 = `select t2.* from salesforce."FM_Slot3_tbl" as t1 join salesforce."View_FM_Basic" as t2 on t1.sfid = t2.sfid order by days_to_expiry__c desc Limit 150`;

    try {
      const data = await strapi.db.connection.context.raw(MainQuery);
      const data2 = await strapi.db.connection.context.raw(MainQuery2);
      const data3 = await strapi.db.connection.context.raw(MainQuery3);

      result = {
        'View_FM_Slot1': data.rows,
        'View_FM_Slot2': data2.rows,
        'View_FM_Slot3': data3.rows
      }
    } catch (error) {
      result = { Msg: "No Result", error: '' };
    }

    return { result };
  },
  async getContact(ctx) {
    const id = ctx.params;
    let result;

    let MainQuery =
      "SELECT * FROM salesforce.contact where sfid = ?";

    try {
      const data = await strapi.db.connection.context.raw(MainQuery, [id.id]);

      result = data.rows;
    } catch (error) {
      result = { Msg: "No Result", error: '' };
    }

    return { result };
  },
  async getDialCode(ctx) {
    const id = ctx.params;
    let result;

    let MainQuery =
      "SELECT CONCAT(name, ' (', dial_code__c, ')') as label, dial_code__c FROM salesforce.fon_country_income_classification__c where dial_code__c is not null order by name asc";

    try {
      const data = await strapi.db.connection.context.raw(MainQuery);

      result = data.rows;
    } catch (error) {
      result = { Msg: "No Result", error: '' };
    }

    return { result };
  },
  async contactSearch(ctx) {
    const request = ctx.request.body;
    var result;
    var result2;
    
    let sortOrder = ''
    let sortdata = ''

    let filters = "zen_member_status__c = 'Active' and isdeleted = false";

    if(request.orderBy == true){
      sortOrder += " order by fon_photo_url__c asc ";
      
    }
    else{
      
      if (request.key.length != 0) {
        if (filters.length > 0) {
          filters += " AND ";
        }

        sortOrder += " ORDER BY rank_exact_n DESC, rank_partial_n DESC, rank_exact_c DESC, rank_partial_c DESC, rank_exact_a DESC, rank_partial_a DESC ";
        
        const finalKey = request.key.trim()
        const keys = finalKey.split(" ");

        sortdata += ", ts_rank(to_tsvector('english', name), to_tsquery('"+ keys.join('&') +"')) AS rank_exact_n, "
        sortdata += "ts_rank(to_tsvector('english', name), to_tsquery('"+ keys.join('|') +"')) AS rank_partial_n, "

        sortdata += "ts_rank(to_tsvector('english', company__c), to_tsquery('"+ keys.join('&') +"')) AS rank_exact_c, "
        sortdata += "ts_rank(to_tsvector('english', company__c), to_tsquery('"+ keys.join('|') +"')) AS rank_partial_c, "

        sortdata += "ts_rank(to_tsvector('english', account_name__c), to_tsquery('"+ keys.join('&') +"')) AS rank_exact_a, "
        sortdata += "ts_rank(to_tsvector('english', account_name__c), to_tsquery('"+ keys.join('|') +"')) AS rank_partial_a "

        //filters += "(name ILIKE  '%" + this.searchText + "%'  or company__c ILIKE  '%" + this.searchText + "%')" //or account_name__c ILIKE  '%" + this.searchText + "%'
        filters += "(to_tsvector('english', name) @@ to_tsquery('"+keys.join('|')+"') "
        filters += " or to_tsvector('english', company__c) @@ to_tsquery('"+keys.join('|')+"') "
        filters += "or to_tsvector('english', account_name__c) @@ to_tsquery('"+keys.join('|')+"') )"
      }

      if (request.iwa_region__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND ";
        }
        filters += "iwa_region__c = '" + request.iwa_region__c + "'"
      }
      if (request.fon_organisation_type__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND ";
        }
        filters += "fon_organisation_type__c = '" + request.fon_organisation_type__c + "'"
      }
      if (request.othercountry?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "othercountry = '" + request.othercountry + "'"
      }

      if (request.fon_gender__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "fon_gender__c = '" + request.fon_gender__c + "'"
      }

      if (request.fon_areas_of_expertise__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "fon_areas_of_expertise__c includes ('" + request.fon_areas_of_expertise__c + "')"
      }

      if (request.fon_years_in_water_sector__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "fon_years_in_water_sector__c = '" + request.fon_years_in_water_sector__c + "'"
      }

      if (request.fon_income_group__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "fon_income_group__c = '" + request.fon_income_group__c + "'"
      }

      if (request.fon_organisation_type__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "fon_organisation_type__c = '" + request.fon_organisation_type__c + "'"
      }

      if (request.iwa_primary_community_group__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "iwa_primary_community_group__c = '" + request.iwa_primary_community_group__c + "'"
      } //IWA_Primary_Community_Group__c

      if (request.iwa_secondary_community_group__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "iwa_secondary_community_group__c = '" + request.iwa_secondary_community_group__c + "'"
      }//IWA_Secondary_Community_Group__c

      if (request.iwa_tertiary_community_group__c?.length > 0) {
        if (filters.length > 0) {
          filters += " AND  ";
        }
        filters += "iwa_tertiary_community_group__c = '" + request.iwa_tertiary_community_group__c + "'"
      }

    }

    

    const MainQuery = "SELECT name, sfid, current_or_most_recent_job__c, primary_job_title__c, mailingcountry, fon_photo_url__c, fon_birthdate__c, company__c, title "+ sortdata +"  FROM salesforce.contact WHERE " + filters + sortOrder + " offset "+ request.offset +" Limit 24";
    const MainQuery2 = "SELECT count(*) FROM salesforce.contact WHERE " + filters ;

    try {
      const data = await strapi.db.connection.context.raw(MainQuery);
      const data2 = await strapi.db.connection.context.raw(MainQuery2);

      result = data.rows;
      result2 = data2.rows;
    } catch (error) {
      result = { Msg: "No Result", error: '', MainQuery };
    }

    try {
      var data123 = ctx;
      data123.request.body = {
        Category: "People",
        Action: "Search People",
        Others: request,
      };
      let addActivity = strapi
        .controller("api::activity-log.activity-log")
        .addActivityInternal(data123);
    } catch (error) {
      console.log(error);
    }

    return { result, result2, MainQuery };
  },
  async updateprivacy(ctx) {
    const request = ctx.request.body;
    let result;
    //, Connect_Plus_Emails__c = "+request.Connect_Plus_Emails__c+", FON_Groups_Communication__c = "+ request.FON_Groups_Communication__c +"
    let MainQuery = "UPDATE salesforce.contact SET fon_privacy_policy_agreement__c = true WHERE sfid = ?";
    try {
      const data = await strapi.db.connection.context.raw(MainQuery, [request.sfid]);

      result = data.rows;
    } catch (error) {
      result = { Msg: "No Result", error: '' };
    }

    try {
      var data123 = ctx;
      data123.request.body = {
        Category: "User",
        Action: "Update Privacy",
        Others: request,
      };
      let addActivity = strapi
        .controller("api::activity-log.activity-log")
        .addActivityInternal(data123);
    } catch (error) {
      console.log(error);
    }


    return { result };
  },
  async updatecontact(ctx) {
    const request = ctx.request.body;
    let result;
    let MainQuery = `UPDATE salesforce.contact SET 
    mailingstreet=?, otherstreet=?,
    zen_billing_address_line_1__c=?, zen_billing_address_line_2__c=?, zen_billing_address_line_3__c=?, otherpostalcode=?, othercity=?, otherstate=?, othercountry=?,
    zen_address_line_1__c=?, zen_address_line_2__c=?, zen_address_line_3__c=?, mailingpostalcode=?, mailingcity=?, mailingstate=?, mailingcountry=?,
    firstname = ?, lastname=?, fon_gender__c=?, description=?, company__c=?, fon_years_in_water_sector__c=?, current_or_most_recent_job__c=?, fon_photo_url__c=?, fon_areas_of_expertise__c=?, orderapi__work_email__c=?, orderapi__personal_email__c=?, orderapi__preferred_email_type__c=?, orderapi__work_phone__c=?, homephone=?, mobilephone=?, orderapi__preferred_phone_type__c=?, fon_iwa_newsletter__c=?, fon_events_webinars_opt_in__c=?, fon_offers_benefits_opt_in__c=?, fon_groups_communication__c=?, connect_plus_emails__c=?, educational_institution__c=?, special__c=?, course_title__c=?, course_start_date__c=?, course_end_date__c=?, fon_organisation_type__c=?, fon_birthdate__c=?, iwa_primary_community_group__c=?, iwa_secondary_community_group__c=?, iwa_tertiary_community_group__c=?, fon_dialling_code__c=?, fon_the_source_communications__c=? WHERE sfid = ?`;
    try {
      const data = await strapi.db.connection.context.raw(MainQuery, [

        request.zen_address_line_1__c +' '+ request.zen_address_line_2__c +' '+ request.zen_address_line_3__c,
        request.zen_billing_address_line_1__c + ' '+ request.zen_billing_address_line_2__c + ' ' + request.zen_billing_address_line_3__c,

        request.zen_billing_address_line_1__c, request.zen_billing_address_line_2__c, request.zen_billing_address_line_3__c, request.otherpostalcode, request.othercity, request.otherstate, request.othercountry,
        request.zen_address_line_1__c, request.zen_address_line_2__c, request.zen_address_line_3__c, request.mailingpostalcode, request.mailingcity, request.mailingstate, request.mailingcountry,
        request.firstname, request.lastname, request.fon_gender__c, request.description, request.company__c, request.fon_years_in_water_sector__c, request.current_or_most_recent_job__c, request.fon_photo_url__c, request.fon_areas_of_expertise__c, 
        request.orderapi__work_email__c,
        request.orderapi__personal_email__c,
        request.orderapi__preferred_email_type__c,
        request.orderapi__work_phone__c,
        request.homephone,
        request.mobilephone,
        request.orderapi__preferred_phone_type__c,
        request.fon_iwa_newsletter__c,
        request.fon_events_webinars_opt_in__c,
        request.fon_offers_benefits_opt_in__c,
        request.fon_groups_communication__c,
        request.connect_plus_emails__c,
        request.educational_institution__c,
        request.special__c,
        request.course_title__c,
        request.course_start_date__c,
        request.course_end_date__c,
        request.fon_organisation_type__c,
        //request.birthdate,
        request.fon_birthdate__c,
        request.iwa_primary_community_group__c,
        request.iwa_secondary_community_group__c,
        request.iwa_tertiary_community_group__c,
        request.fon_dialling_code__c,
        request.fon_the_source_communications__c,
        request.sfid]);

      result = data.rows;


        let MainQuery2 = "UPDATE salesforce.pagesapi__community_group_member__c SET  iwa_is_tertiary__c = false,  iwa_is_primary__c=false,  iwa_is_secondary__c=false WHERE pagesapi__contact__c = ?";
        const data2 = await strapi.db.connection.context.raw(MainQuery2, [request.sfid]);

        if(request.iwa_primary_community_group__c !== null){
          let MainQuery2 = "UPDATE salesforce.pagesapi__community_group_member__c SET  iwa_is_primary__c=true WHERE pagesapi__contact__c = ? and pagesapi__community_group__c = ?";
          const data3 = await strapi.db.connection.context.raw(MainQuery2, [request.sfid, request.iwa_primary_community_group__c]);
        }

        if(request.iwa_secondary_community_group__c !== null){
          let MainQuery2 = "UPDATE salesforce.pagesapi__community_group_member__c SET  iwa_is_secondary__c=true WHERE pagesapi__contact__c = ? and pagesapi__community_group__c = ?";
          const data4 = await strapi.db.connection.context.raw(MainQuery2, [request.sfid, request.iwa_secondary_community_group__c]);
        }

        if(request.iwa_tertiary_community_group__c !== null){
          let MainQuery2 = "UPDATE salesforce.pagesapi__community_group_member__c SET  iwa_is_tertiary__c=true WHERE pagesapi__contact__c = ? and pagesapi__community_group__c = ?";
          const data5 = await strapi.db.connection.context.raw(MainQuery2, [request.sfid, request.iwa_tertiary_community_group__c]);
        }

        try {
          var data123 = ctx;
          data123.request.body = {
            Category: "User",
            Action: "Update User",
            Others: request,
          };
          let addActivity = strapi
            .controller("api::activity-log.activity-log")
            .addActivityInternal(data123);
        } catch (error) {
          console.log(error);
        }
    


    } catch (error) {
      console.log(error)
      result = { Msg: "No Result", error: error };
    }

    return { result };
  },
  async updateFMMember(ctx) {
    const request = ctx.request.body;
    let result;
    let MainQuery = "UPDATE salesforce.contact SET iwa_week_number__c=null, fm_slot__c = null WHERE iwa_week_number__c = ?";
    

    let MainQuery1 = "UPDATE salesforce.contact SET iwa_week_number__c=?, fm_slot__c = 1 WHERE sfid = ?";
    let MainQuery2 = "UPDATE salesforce.contact SET iwa_week_number__c=?, fm_slot__c = 2 WHERE sfid = ?";
    let MainQuery3 = "UPDATE salesforce.contact SET iwa_week_number__c=?, fm_slot__c = 3 WHERE sfid = ?";
    try {
      const data = await strapi.db.connection.context.raw(MainQuery, [request.iwa_week_number__c]);
      const data1 = await strapi.db.connection.context.raw(MainQuery1, [request.iwa_week_number__c, request.sfid1]);
      const data2 = await strapi.db.connection.context.raw(MainQuery2, [request.iwa_week_number__c, request.sfid2]);
      const data3 = await strapi.db.connection.context.raw(MainQuery3, [request.iwa_week_number__c, request.sfid3]);
    } catch (error) {
      result = { Msg: "Error Pls try again...!", error: '' };
    }

    return { Msg: "Updated..!", error: '' };
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
