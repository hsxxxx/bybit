module.exports = {
  apps: [
    {
      name: "bybit-collector",
      cwd: __dirname + "/collector",
      script: "dist/index.js",
      interpreter: "/home/hsxxxx/.nvm/versions/node/v20.20.0//bin/node",
      autorestart: true,
      restart_delay: 2000,
      env_file: __dirname + "/collector/.env",
      env: { NODE_ENV: "production" },
    },
    {
      name: "bybit-builder",
      cwd: __dirname + "/builder",
      script: "dist/index.js",
      interpreter: "/home/hsxxxx/.nvm/versions/node/v20.20.0//bin/node",
      autorestart: true,
      restart_delay: 2000,
      env_file: __dirname + "/builder/.env",
      env: { NODE_ENV: "production" },
    },
  ],
};