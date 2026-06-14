package apis

import (
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/realtime"
)

func bindRealtimeApi(app core.App, group *echo.Group) {
	api := realtimeApi{app: app}

	group.GET("/realtime", api.connect, ActivityLogger(app))
	group.POST("/realtime", api.submitSubscriptions, ActivityLogger(app))
}

type realtimeApi struct {
	app core.App
}

func (api *realtimeApi) connect(c echo.Context) error {
	c.Response().Header().Set(echo.HeaderContentType, "text/event-stream")
	c.Response().Header().Set(echo.HeaderCacheControl, "no-cache")
	c.Response().Header().Set(echo.HeaderConnection, "keep-alive")
	c.Response().Header().Set(echo.HeaderXContentTypeOptions, "nosniff")

	clientId := c.QueryParam("clientId")
	if clientId == "" {
		clientId = c.FormValue("clientId")
	}
	if clientId == "" {
		clientId = realtime.GenerateClientId()
	}

	client := realtime.NewDefaultClient(clientId)

	client.Set("admin", c.Get(ContextAdminKey))
	client.Set("authRecord", c.Get(ContextAuthRecordKey))

	api.app.Realtime().Register(client)
	defer func() {
		api.app.Realtime().Unregister(clientId)
	}()

	// send connection established event
	client.Channel() <- &realtime.Message{
		Name: "connect",
		Data: []byte(`{"clientId":"` + clientId + `"}`),
	}

	// keep connection alive
	c.Response().WriteHeader(http.StatusOK)
	c.Response().Flush()

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case msg, ok := <-client.Channel():
			if !ok {
				return nil
			}
			if _, err := c.Response().Write(msg.Bytes()); err != nil {
				return nil
			}
			c.Response().Flush()
		}
	}
}

func (api *realtimeApi) submitSubscriptions(c echo.Context) error {
	data := struct {
		ClientId      string   `json:"clientId" form:"clientId" query:"clientId"`
		Subscriptions []string `json:"subscriptions" form:"subscriptions" query:"subscriptions"`
	}{}

	if err := c.Bind(&data); err != nil {
		return failBind(err)
	}

	if data.ClientId == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "clientId is required"})
	}

	client, err := api.app.Realtime().GetClient(data.ClientId)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"message": "client not found"})
	}

	// update subscriptions
	client.SetSubscriptions(data.Subscriptions)

	// update auth context
	client.Set("admin", c.Get(ContextAdminKey))
	client.Set("authRecord", c.Get(ContextAuthRecordKey))

	return c.NoContent(http.StatusNoContent)
}
