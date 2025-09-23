import express from "express";
import fs from "fs";
import { spawn } from "child_process";
import { getOutputPath } from "../utils/paths.js";

const router = express.Router();
const PYTHON_PATH = "C:/Users/Ebad Ur Rehman/AppData/Local/Programs/Python/Python313/python.exe";

// Expect key: file
router.post("/", async (req, res) => {
  try {
    if (!req.files || !req.files.file) {
      return res.status(400).send("Please upload a file with key 'file'.");
    }

    const { file } = req.files;
    const outputPath = getOutputPath("processed_single.xlsx");

    // Run Python
    const py = spawn(PYTHON_PATH, [
      "./python_scripts/process_single.py",
      file.tempFilePath,
      outputPath
    ]);
let stderr = "";
    let finished = false;

    // Timeout after 60s
    const timeout = setTimeout(() => {
      if (!finished) {
        py.kill("SIGKILL"); // force kill python
        return res.status(504).send("Processing timed out after 60s.");
      }
    }, 60000);
  
    py.stderr.on("data", (d) => (stderr += d.toString()));

    py.on("close", (code) => {
      if (code !== 0) {
        console.error("Python error:", stderr);
        return res.status(500).send("Processings failed.");
      }

      // Send processed Excel file back
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="processed_single.xlsx"`
      );

      const stream = fs.createReadStream(outputPath);
      stream.pipe(res);

      stream.on("close", () => {
        fs.existsSync(outputPath) && fs.unlinkSync(outputPath);
      });
      stream.on("error", () => {
        fs.existsSync(outputPath) && fs.unlinkSync(outputPath);
      });
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Server error.");
  }
});

export default router;
