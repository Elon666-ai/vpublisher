package utils

const (
	APP_NAME = "VLB"
	VERSION  = "v1.0.4"

	AppID     = "appbd4cd2aa7be8"
	AppSecret = "8445cc80cb9375385cc16c3e98dfe952"
)

var versionModification = map[string]string{
	"v1.0.0": "init release",
}

func GetVersionModification() string {
	return versionModification[VERSION]
}

const (
	ENV_FILE  = ".env"
	CONF_FILE = "conf/vpublisher.yml"

	NODE_CAPACITY_publisher = 100
	CMD_TYPE_startPub       = "COMMAND_TYPE_START_PUB"
	CMD_TYPE_stopPub        = "COMMAND_TYPE_STOP_PUB"
	CMD_TYPE_originDown     = "COMMAND_TYPE_ORIGIN_DOWN"
	CMD_TYPE_originUp       = "COMMAND_TYPE_ORIGIN_UP"
	CMD_TYPE_queryPubPts    = "COMMAND_TYPE_QUERY_PUB_PTS"
	CMD_TYPE_indication     = "COMMAND_TYPE_INDICATION"
	CMD_TYPE_response       = "COMMAND_TYPE_RESPONSE"
)

/*
Hello,

Your application has been registered.

AppID: appbd4cd2aa7be8
AppSecret: 8445cc80cb9375385cc16c3e98dfe952
Trial Balance: 50U

Please keep the AppSecret secure.
*/
