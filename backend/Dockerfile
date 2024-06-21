# Use Node.js LTS version as base image for backend
FROM node:lts AS backend-builder

# Set working directory for backend
WORKDIR /app/backend

# Copy package.json and package-lock.json to install dependencies
COPY package*.json ./

# Install backend dependencies
RUN npm install

# Copy the rest of the backend application code
COPY . .

# Expose the port on which the backend app runs (if applicable)
# EXPOSE 3001

# Command to run the backend app
CMD ["npm", "start"]
