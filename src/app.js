const express = require("express");
const statsRoutes = require("./routes/statsRoutes");

const app = express();

app.use(express.json());
app.use("/", statsRoutes);

module.exports = app;
