module.exports = {
  apps: [
    {
      name: 'sendzyy-backend',
      script: 'server.js',
      cwd: '/www/wwwroot/your-app/backend', // update this path to your actual path on EC2
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        PORT: 3000,
        WEBHOOK_SECRET_KEY: '7d9beab6f56c5ff212b109fdb859604f8c9e9d698b3e5e42ba769de7e3d7a568',
      },
    },
  ],
};
