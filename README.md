# tap-eloqua

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Eloqua's [bulk export](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAB/Developers/BulkAPI/Tutorials/Export.htm) and [application RESTful](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/index.html) APIs.
- Extracts the following resources from Eloqua
  - Bulk
     - [Accounts](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-bulk-2.0-accounts-exports-post.html)
     - [Activities](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-bulk-2.0-activities-exports-post.html)
     - [Contacts](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-bulk-2.0-contacts-exports-post.html)
     - [Custom Objects](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-bulk-2.0-customobjects-parentid-exports-post.html)
  - Application REST
     - [Assets](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-rest-2.0-assets-externals-get.html)
     - [Campaigns](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-rest-2.0-assets-campaigns-get.html)
     - [Emails](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-rest-1.0-assets-emails-get.html)
     - [Forms](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-rest-1.0-assets-forms-get.html)
     - [Visitors](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/op-api-rest-2.0-data-visitors-get.html)
- Outputs the schema for each resource

## Configuration

This tap requires a `config.json` which specifies details regarding [API OAuth 2 authentication](https://docs.oracle.com/cloud/latest/marketingcs_gs/OMCAC/Authentication_Auth.html) and cutoff date for syncing historical data. See [config.sample.json](config.sample.json) for an example.

To run `tap-eloqua` with the configuration file, use this command:

```bash
› tap-eloqua -c my-config.json
```

---

Copyright &copy; 2018 Stitch