# Stage 1: install deps + dev server (also used as build base)
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .

# Stage 2: production build
FROM builder AS build-stage
RUN npm run build
# Stamp a version file so the browser can detect redeploys
RUN echo "{\"buildTime\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" > /app/dist/version.json

# Stage 3: serve with nginx
FROM nginx:alpine AS production
COPY --from=build-stage /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
