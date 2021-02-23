module.exports = {
	STORAGE_LOCATIONS:[
		{"path":"./blocks1/"},
		{"path":"./blocks2/"}
	],
	BLOCK_SIZE: 1048576,
	LOG_LEVEL: 0,
	SERVER_PORT: 7302,
	REQUEST_TIMEOUT: 30, // minutes
	CONFIGURED_STORAGE: "linode-object-storage",
  LINODE_OBJECT_STORAGE: {
    ENDPOINT: "us-east-1.linodeobjects.com",
    BUCKET: "rabble-dev-scratch",
    AUTHENTICATION: {
      ACCESS_KEY_ID: "7G6RJV54GTJWXPFKSU9X",
      SECRET_ACCESS_KEY: "rNyLmYxN8sgmYS78eK4WUfYT4L0XezNJb23NSsPB",
    }
  }


};
