# Use Node.js LTS version as base image for frontend build
FROM node:lts AS frontend-builder

# Set working directory for frontend
WORKDIR /app/frontend

# Copy package.json and package-lock.json to install dependencies
COPY package.json package-lock.json ./

# Install dependencies
RUN npm install

# Copy the rest of the frontend application code
COPY . .

# Build the React app using Vite
RUN npm run build

# Use lightweight Node.js image for serving frontend
FROM node:lts-alpine AS frontend-server

# Set working directory for serving frontend
WORKDIR /app/frontend

# Ensure the build directory exists
RUN mkdir -p /app/frontend/build

# Copy the built frontend app from the builder stage
COPY --from=frontend-builder /app/frontend/dist ./build

# Install serve to run the production server
RUN npm install -g serve

# Expose the port on which the frontend app runs
EXPOSE 3000

# Command to run the frontend app using serve
CMD ["serve", "-s", "build"]
