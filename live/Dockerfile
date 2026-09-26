# Build context is the repository root (see compose.yml).
FROM node:24-alpine AS web
WORKDIR /web
COPY live/web/package.json live/web/package-lock.json ./
RUN npm ci --no-fund --no-audit
COPY live/web/ ./
RUN npm run build

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG ORLEANS_VERSION=10.3.1
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props ./
COPY src/Poc/Poc.csproj src/Poc/
COPY src/Live/Live.csproj src/Live/
RUN dotnet restore src/Live -p:OrleansVersion=$ORLEANS_VERSION
COPY src/ src/
RUN dotnet publish src/Live -c Release -p:OrleansVersion=$ORLEANS_VERSION --no-restore -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
COPY --from=web /web/dist wwwroot/
ENTRYPOINT ["dotnet", "Live.dll"]
