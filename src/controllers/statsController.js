const pool = require("../db");

async function getOrderStats(req, res) {
  try {
    const result = await pool.query(
      "SELECT COUNT(*)::int AS total_orders, COALESCE(AVG(amount), 0) AS average_order_amount FROM orders"
    );
    const { total_orders, average_order_amount } = result.rows[0];
    return res.json({
      status: "success",
      data: {
        totalOrders: total_orders,
        averageOrderAmount: parseFloat(Number(average_order_amount).toFixed(2)),
      },
    });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: "Failed to retrieve order stats",
    });
  }
}

async function getUserStats(req, res) {
  try {
    const result = await pool.query(
      "SELECT COUNT(*)::int AS total_users, COALESCE(AVG(EXTRACT(EPOCH FROM (NOW() - signup_date)) / 86400), 0) AS average_signup_age FROM users"
    );
    const { total_users, average_signup_age } = result.rows[0];
    return res.json({
      status: "success",
      data: {
        totalUsers: total_users,
        averageSignupAge: parseFloat(Number(average_signup_age).toFixed(2)),
      },
    });
  } catch (error) {
    return res.status(500).json({
      status: "error",
      message: "Failed to retrieve user stats",
    });
  }
}

module.exports = { getOrderStats, getUserStats };
