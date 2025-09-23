import express from "express";
import jwt from "jsonwebtoken";
import { sql, poolPromise } from "../config/db.js";

const router = express.Router();

// Middleware to verify JWT
function verifyToken(req, res, next) {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    return res.status(401).json({ message: "No token provided" });
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    console.error("JWT verify error:", err.name, err.message);

    if (err.name === "TokenExpiredError") {
      return res.status(401).json({ message: "SignIn Again" });
    }

    return res.status(403).json({ message: "Invalid token" });
  }
}

// Login API (no token check here)
router.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ message: "Email and password are required" });
    }

    const pool = await poolPromise;
    const result = await pool
      .request()
      .input("email", sql.VarChar, email)
      .input("password", sql.VarChar, password)
      .query(`
        SELECT u.UserID, u.Email, r.RoleName
        FROM Users u
        INNER JOIN Roles r ON u.RoleID = r.RoleID
        WHERE u.Email = @email AND u.Password = @password
      `);

    if (result.recordset.length === 0) {
      return res.status(401).json({ message: "Invalid email or password" });
    }

    const user = result.recordset[0];

    const token = jwt.sign(
      { id: user.UserID, email: user.Email, role: user.RoleName },
      process.env.JWT_SECRET,
      { expiresIn: "12H" } // short expiry for demo
    );

    res.json({ message: "Login successful", token, user });
  } catch (err) {
    console.error("Login error:", err);
    res.status(500).json({ message: "Server error" });
  }
});

// Example of a protected real route
router.get("/users", verifyToken, async (req, res) => {
  try {
    const pool = await poolPromise;
    const result = await pool.request().query("SELECT UserID, Email FROM Users");
    res.json(result.recordset);
  } catch (err) {
    console.error("Users fetch error:", err);
    res.status(500).json({ message: "Server error" });
  }
});

export default router;
