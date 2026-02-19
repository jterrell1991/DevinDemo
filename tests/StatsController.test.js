const request = require("supertest");
const app = require("../src/app");
const pool = require("../src/db");

jest.mock("../src/db", () => ({
  query: jest.fn(),
}));

afterAll(() => {
  jest.restoreAllMocks();
});

describe("GET /orders/stats", () => {
  it("should return total orders and average order amount", async () => {
    pool.query.mockResolvedValueOnce({
      rows: [{ total_orders: 10, average_order_amount: "55.50" }],
    });

    const res = await request(app).get("/orders/stats");

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({
      status: "success",
      data: {
        totalOrders: 10,
        averageOrderAmount: 55.5,
      },
    });
    expect(pool.query).toHaveBeenCalledWith(
      "SELECT COUNT(*)::int AS total_orders, COALESCE(AVG(amount), 0) AS average_order_amount FROM orders"
    );
  });

  it("should return zeros when no orders exist", async () => {
    pool.query.mockResolvedValueOnce({
      rows: [{ total_orders: 0, average_order_amount: "0" }],
    });

    const res = await request(app).get("/orders/stats");

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({
      status: "success",
      data: {
        totalOrders: 0,
        averageOrderAmount: 0,
      },
    });
  });

  it("should return 500 on database error", async () => {
    pool.query.mockRejectedValueOnce(new Error("DB connection failed"));

    const res = await request(app).get("/orders/stats");

    expect(res.statusCode).toBe(500);
    expect(res.body).toEqual({
      status: "error",
      message: "Failed to retrieve order stats",
    });
  });
});

describe("GET /users/stats", () => {
  it("should return total users and average signup age", async () => {
    pool.query.mockResolvedValueOnce({
      rows: [{ total_users: 25, average_signup_age: "45.75" }],
    });

    const res = await request(app).get("/users/stats");

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({
      status: "success",
      data: {
        totalUsers: 25,
        averageSignupAge: 45.75,
      },
    });
    expect(pool.query).toHaveBeenCalledWith(
      "SELECT COUNT(*)::int AS total_users, COALESCE(AVG(EXTRACT(EPOCH FROM (NOW() - signup_date)) / 86400), 0) AS average_signup_age FROM users"
    );
  });

  it("should return zeros when no users exist", async () => {
    pool.query.mockResolvedValueOnce({
      rows: [{ total_users: 0, average_signup_age: "0" }],
    });

    const res = await request(app).get("/users/stats");

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({
      status: "success",
      data: {
        totalUsers: 0,
        averageSignupAge: 0,
      },
    });
  });

  it("should return 500 on database error", async () => {
    pool.query.mockRejectedValueOnce(new Error("DB connection failed"));

    const res = await request(app).get("/users/stats");

    expect(res.statusCode).toBe(500);
    expect(res.body).toEqual({
      status: "error",
      message: "Failed to retrieve user stats",
    });
  });

  it("should handle large signup age values", async () => {
    pool.query.mockResolvedValueOnce({
      rows: [{ total_users: 1000, average_signup_age: "365.123456" }],
    });

    const res = await request(app).get("/users/stats");

    expect(res.statusCode).toBe(200);
    expect(res.body.status).toBe("success");
    expect(res.body.data.totalUsers).toBe(1000);
    expect(res.body.data.averageSignupAge).toBe(365.12);
  });
});
