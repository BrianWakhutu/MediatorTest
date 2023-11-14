'use strict'

import express from 'express'
import csvParser  from 'csv-parser'
import sql from 'mssql'
/* const csvParser = require('csv-parser');*/
//const sql = require('mssql'); 

// The OpenHIM Mediator Utils is an essential package for quick mediator setup.
// It handles the OpenHIM authentication, mediator registration, and mediator heartbeat.
import {
  registerMediator,
  activateHeartbeat,
  fetchConfig
} from 'openhim-mediator-utils'

// The mediatorConfig file contains some basic configuration settings about the mediator
// as well as details about the default channel setup.
import mediatorConfig, { urn } from './mediatorConfig.json'

// The config details here are used to authenticate and register the mediator with the OpenHIM instance
const openhimConfig = {
  username: 'root@openhim.org',
  password: 'Xkemricdc@123',
  apiURL: 'https://openhim-core:8080',
  trustSelfSigned: true,
  urn
}

const app = express();
app.use(express.json());

const sqlConfig = {
  user: 'sa',
  password: 'Xkemricdc@123',
  server: '4VHM2T3-KEN',
  database: 'DMI_Cholera_Stagging',
};

// Any request regardless of request type or url path to the mediator port will be caught here
// and trigger the Hello World response.
/* app.all('*', (_req, res) => {
  res.send('Hello World')
}); */
app.post('/receive-csv', (req, res) => {
  let data = [];
  let sql ;
  req.pipe(csvParser())
     .on('data', (row) => data.push(row))
     .on('end', async () => {
         try {
             await sql.connect(sqlConfig);
             for (const row of data) {
                 // Assume CSV has columns 'column1', 'column2'
                 const query = `INSERT INTO [dbo].[Cholera]
                 ([ID_NO]
                 ,[PATIENT_NAME]
                 ,[OUT_PATIENT]
                 ,[IN_PATIENT]
                 ,[COUNTY]
                 ,[SUB_COUNTY]
                 ,[VILLAGE_TOWN_AND_NEIGHBORHOOD]
                 ,[WARD]
                 ,[NAME_OF_HEALTH_FACILITY]
                 ,[CONTACTTELNUMBER]
                 ,[SEX]
                 ,[AGE]
                 ,[DATE_SEEN_AT_HEALTH_FACILITY]
                 ,[DATE_OF_ONSET_OF_DISEASES]
                 ,[EPI_WEEK]
                 ,[NUMBER_OF_DOSES_OF_VACCINES]
                 ,[Sample_Collected_Yes_no]
                 ,[Date_Sample_collected]
                 ,[RDT_Yes_No]
                 ,[RDT_Results_Positive_Negative]
                 ,[Positive_by_RDT]
                 ,[CULTURE_Yes_No]
                 ,[Culture_Results_Positive_Negative]
                 ,[Positive_by_Culture2]
                 ,[Sero_Type]
                 ,[A_Alive]
                 ,[D_Dead]
                 ,[Date_of_Death]
                 ,[Patients_status_Admiited_Discharged_Out_Patient_Abscoded]
                 ,[Date_of_Discharge]
                 ,[More_Infor]
                 ,[Age_analysis]
                 ,[Lab_of_Diagnosis]
                 ,[column34]
                 ,[Place_of_Death_Facility_Community]) VALUES ('${row.ID_NO}', 
                 '${row.PATIENT_NAME}',${row.PATIENT_NAME}',${row.OUT_PATIENT}',${row.IN_PATIENT}'
                 ,${row.COUNTY}',${row.SUB_COUNTY}',${row.VILLAGE_TOWN_AND_NEIGHBORHOOD}'
                 ,${row.WARD}',${row.NAME_OF_HEALTH_FACILITY},${row.CONTACTTELNUMBER}',${row.SEX}',${row.AGE}'
                 ,${row.DATE_SEEN_AT_HEALTH_FACILITY}',${row.DATE_OF_ONSET_OF_DISEASES},${row.EPI_WEEK}',${row.NUMBER_OF_DOSES_OF_VACCINES}',${row.Sample_Collected_Yes_no}'
                 ,${row.Date_Sample_collected}',${row.RDT_Yes_No},${row.RDT_Results_Positive_Negative}',${row.Positive_by_RDT}',${row.CULTURE_Yes_No}'
                 ,${row.Culture_Results_Positive_Negative}',${row.Positive_by_Culture2},${row.Sero_Type}',${row.A_Alive},${row.D_Dead}',${row.Date_of_Death}'
                 ,${row.Patients_status_Admiited_Discharged_Out_Patient_Abscoded}'
                 ,${row.Date_of_Discharge}',${row.More_Infor}
                 ,${row.Age_analysis}'
                 ,${row.Lab_of_Diagnosis}',${row.column34}
                 ,${row.column34}',${row.Place_of_Death_Facility_Community}
                 )`;
                 await sql.query(query);
             }
             res.send('CSV data processed and stored in SQL Server Database');
         } catch (err) {
             console.error(err);
             res.status(500).send('An error occurred');
         }
     });
});
app.listen(3000, () => {
  console.log('Server listening on port 3000...')

  mediatorSetup()
})

const mediatorSetup = () => {
  // The purpose of registering the mediator is to allow easy communication between the mediator and the OpenHIM.
  // The details received by the OpenHIM will allow quick channel setup which will allow tracking of requests from
  // the client through any number of mediators involved and all the responses along the way(if the mediators are
  // properly configured). Moreover, if the request fails for any reason all the details are recorded and it can
  // be replayed at a later date to prevent data loss.
  registerMediator(openhimConfig, mediatorConfig, err => {
    if (err) {
      throw new Error(`Failed to register mediator. Check your Config. ${err}`)
    }

    console.log('Successfully registered mediator!')

    fetchConfig(openhimConfig, (err, initialConfig) => {
      if (err) {
        throw new Error(`Failed to fetch initial config. ${err}`)
      }
      console.log('Initial Config: ', JSON.stringify(initialConfig))

      // The activateHeartbeat method returns an Event Emitter which allows the mediator to attach listeners waiting
      // for specific events triggered by OpenHIM responses to the mediator posting its heartbeat.
      const emitter = activateHeartbeat(openhimConfig)

      emitter.on('error', err => {
        console.error(`Heartbeat failed: ${err}`)
      })

      // The config events is emitted when the heartbeat request posted by the mediator returns data from the OpenHIM.
      emitter.on('config', newConfig => {
        console.log('Received updated config:', JSON.stringify(newConfig))
      })
    })
  })
}
