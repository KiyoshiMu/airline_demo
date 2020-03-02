// {
//     "name": "sample-cloud-storage",
//     "version": "0.0.1",
//     "dependencies": {
//       "@google-cloud/storage": "^1.6.0",
//       "googleapis": "^47.0.0"
//     }
//   }
const { google } = require("googleapis");
const dataflow = google.dataflow("v1b3");

exports.main = async (data, _) => {
  const file = data;
  if (file.name.includes("tmp/csvs")) {
    const input = `gs://${file.bucket}/${file.name}`;
    const output = "eeeooosss:flight.rawflight";
    const airport = "gs://eoseoseos/tmp/airports.csv";
    const TEMPLATE_BUCKET = "eoseoseos";
    const TEMPLATE_NAME = "flightFlow";
    const tmpLocation = `gs://${TEMPLATE_BUCKET}/tmp`;
    const templatePath = `gs://${TEMPLATE_BUCKET}/templates/${TEMPLATE_NAME}`;
    const jobName = new Date().toISOString();

    const request = {
      projectId: "eeeooosss",
      validateOnly: false,
      requestBody: {
        jobName: jobName,
        parameters: {
          input: input,
          output: output,
          airport: airport
        },
        environment: {
          tempLocation: tmpLocation
        }
      },
      gcsPath: templatePath
    };

    const auth = await google.auth.getClient({
      scopes: ["https://www.googleapis.com/auth/cloud-platform"]
    });
    request.auth = auth;
    const res = await dataflow.projects.templates.launch(request);
    console.log(res.data);
  }
};
