module.exports = {
  apps: [
    {
      name: "bybit-collector",
      cwd: __dirname + "/apps/collector",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env_file: __dirname + "/apps/collector/.env",
      env: {
        NODE_ENV: "production",
      },
    },
    {
      name: "bybit-builder",
      cwd: __dirname + "/apps/builder",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env_file: __dirname + "/apps/builder/.env",
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};