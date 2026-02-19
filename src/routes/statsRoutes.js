const express = require("express");
const { getOrderStats, getUserStats } = require("../controllers/statsController");

const router = express.Router();

router.get("/orders/stats", getOrderStats);
router.get("/users/stats", getUserStats);

module.exports = router;
