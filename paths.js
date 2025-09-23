import fs from "fs";
import path from "path";

const uploadsDir = path.join(process.cwd(), "uploads");
const outputDir = path.join(process.cwd(), "output");

export function ensureFolders() {
  if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
}

export function getOutputPath(name = "processed.xlsx") {
  return path.join(outputDir, name);
}
