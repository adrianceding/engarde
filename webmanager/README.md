# engarde web manager
This is an Angular interface to manage an engarde instance. It is built into the engarde binaries.

## Developing
Just run `npm install` and `npm start`.

By default, requests to the API are forwarded to http://127.0.0.1:9001, so be sure to set up an engarde instance with web management enabled on port 9001.

During development, you should leave authentication disabled for better testing. In the builds, authentication is handled by the embedded webserver itself, so the frontend doesn't need to worry about it at the moment.
