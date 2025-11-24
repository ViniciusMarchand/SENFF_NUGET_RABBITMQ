FROM mcr.microsoft.com/dotnet/sdk:10.0

ENV ASPNETCORE_ENVIRONMENT=Development
ENV DOTNET_USE_POLLING_FILE_WATCHER=1
ENV ASPNETCORE_URLS=http://+:80

WORKDIR /src

COPY ["WebApp.csproj", "."]
RUN dotnet restore "WebApp.csproj"

COPY . .

EXPOSE 80

ENTRYPOINT ["dotnet", "watch", "run", "--urls", "http://0.0.0.0:80"]