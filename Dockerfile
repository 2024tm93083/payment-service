# --- CUSTOMER SERVICE DOCKERFILE ---
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY src ./src
COPY .env .env

EXPOSE 6000
CMD ["node", "src/main.js"]
