module.exports = {
  apps: [
    {
      name: "collector",
      cwd: __dirname + "/collector",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production" }
    },
    {
      name: "builder",
      cwd: __dirname + "/builder",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production" }
    },
    {
      name: "sinker",
      cwd: __dirname + "/sinker",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production" }
    },
    {
      name: "viewer",
      cwd: __dirname + "/viewer",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: {
        NODE_ENV: "production",
        PORT: "3100",
        PORT_WS: "3101"
      }
    }
  ]
};
