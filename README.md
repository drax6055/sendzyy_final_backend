# Sendzyy Backend

Node.js Express backend using MongoDB/Mongoose for WhatsApp messaging and Meta embedded signup.

## Getting Started
1. Install dependencies:
   ```bash
   npm install
   ```
2. Configure `.env` based on `.env.example`.
3. Start development server:
   ```bash
   npm start
   ```

## CI/CD Deployment
This project has a GitHub Actions CI/CD pipeline configured to automatically deploy backend updates to AWS aaPanel using PM2 when changes are pushed to the `main` branch.
