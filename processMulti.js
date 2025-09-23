import express from "express";
import fs from "fs";
import { spawn } from "child_process";
import { getOutputPath } from "../utils/paths.js";

import jwt from "jsonwebtoken";

// Extract user from token



const router = express.Router();
const PYTHON_PATH = "D:\\EBAD-PROFILEDATA\\Documents\\ana\\python.exe";

router.post("/", async (req, res) => {
  try {
    if (
      !req.files ||
      !req.files.file1 ||
      !req.files.file2 ||
      !req.files.file3 ||
      !req.files.file4
    ) {
      return res.status(400).send("Please upload all 4 files (file1..file4).");
    }

    const { file1, file2, file3, file4 } = req.files;
    const outputPath = getOutputPath("processed_multi.xlsx");
    const authHeader = req.headers.authorization;
    const token = authHeader.split(" ")[1];
    const user = jwt.verify(token, process.env.JWT_SECRET);
  // processMulti.js (only the spawn args change)
const py = spawn(PYTHON_PATH, [
  "./python_scripts/process_multi.py",
  file1.tempFilePath, file1.name,
  file2.tempFilePath, file2.name,
  file3.tempFilePath, file3.name,
  file4.tempFilePath, file4.name,
  outputPath,
  user.id
]);

    let stderr = "";
    let responded = false;

    // Timeout after 60s
    
    const timeout = setTimeout(() => {
      if (!responded) {
        py.kill("SIGKILL");
        responded = true;
        return res.status(504).send("Processing timed out after 60s.");
      }
    }, 60000);

    py.stderr.on("data", (d) => (stderr += d.toString()));

    py.on("close", (code) => {
      clearTimeout(timeout);
      if (responded) return;

      if (code !== 0) {
        console.error("Python error:", stderr);
        responded = true;
        return res.status(500).send("Processing failed.");
      }

      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.setHeader(
        "Content-Disposition",
        'attachment; filename="processed_multi.xlsx"'
      );

      const stream = fs.createReadStream(outputPath);
      stream.pipe(res);

      stream.on("end", () => {
        fs.existsSync(outputPath) && fs.unlinkSync(outputPath);
      });

      stream.on("error", (err) => {
        console.error("Stream error:", err);
        if (!responded) {
          responded = true;
          res.status(500).send("Error streaming file.");
        }
        fs.existsSync(outputPath) && fs.unlinkSync(outputPath);
      });
    });
  } catch (err) {
    console.error(err);
    if (!res.headersSent) res.status(500).send("Server error.");
  }
});

export default router;
