package framework

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/labstack/echo/v4"
)

type payload struct {
	ID   string `form:"id" uri:"id" param:"id" query:"id"`
	Name string `form:"name" query:"name"`
	Tag  string `form:"tag" query:"tag"`
	Note string `form:"note" query:"note"`
}

func ginHandler() http.Handler {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/bind/:id", func(c *gin.Context) {
		var p payload
		if err := c.ShouldBind(&p); err != nil {
			c.String(400, err.Error())
			return
		}
		c.String(200, "id=%s name=%s tag=%s", p.ID, p.Name, p.Tag)
	})
	r.POST("/form", func(c *gin.Context) {
		if err := c.Request.ParseForm(); err != nil {
			c.String(400, err.Error())
			return
		}
		c.String(200, "name=%s formValue=%s raw=%s", c.PostForm("name"), c.Request.FormValue("name"), c.Request.PostForm.Get("name"))
	})
	r.POST("/upload", func(c *gin.Context) {
		var p payload
		if err := c.ShouldBind(&p); err != nil {
			c.String(400, err.Error())
			return
		}
		file, err := c.FormFile("upload")
		if err != nil {
			c.String(400, err.Error())
			return
		}
		c.String(200, "note=%s file=%s", p.Note, file.Filename)
	})
	r.POST("/multipart", func(c *gin.Context) {
		form, err := c.MultipartForm()
		if err != nil {
			c.String(400, err.Error())
			return
		}
		f, err := c.FormFile("upload")
		if err != nil {
			c.String(400, err.Error())
			return
		}
		c.String(200, "note=%s file=%s count=%d", form.Value["note"][0], f.Filename, len(form.File["upload"]))
	})
	return r
}

func echoHandler() http.Handler {
	e := echo.New()
	e.POST("/bind/:id", func(c echo.Context) error {
		var p payload
		if err := c.Bind(&p); err != nil {
			return c.String(400, err.Error())
		}
		return c.String(200, fmt.Sprintf("id=%s name=%s tag=%s", p.ID, p.Name, p.Tag))
	})
	e.POST("/form", func(c echo.Context) error {
		if err := c.Request().ParseForm(); err != nil {
			return c.String(400, err.Error())
		}
		return c.String(200, fmt.Sprintf("name=%s raw=%s", c.FormValue("name"), c.Request().PostForm.Get("name")))
	})
	e.POST("/upload", func(c echo.Context) error {
		var p payload
		if err := c.Bind(&p); err != nil {
			return c.String(400, err.Error())
		}
		f, err := c.FormFile("upload")
		if err != nil {
			return c.String(400, err.Error())
		}
		return c.String(200, fmt.Sprintf("note=%s file=%s", p.Note, f.Filename))
	})
	e.POST("/multipart", func(c echo.Context) error {
		fm, err := c.MultipartForm()
		if err != nil {
			return c.String(400, err.Error())
		}
		f, err := c.FormFile("upload")
		if err != nil {
			return c.String(400, err.Error())
		}
		return c.String(200, fmt.Sprintf("note=%s file=%s count=%d", c.FormValue("note"), f.Filename, len(fm.File["upload"])))
	})
	return e
}

// These cases preserve the binding and multipart scenarios in gin_bind,
// echo_bind, gin_gap and echo_gap_test.go from the legacy adaptor tests.
func bindCases(t *testing.T, h http.Handler) {
	t.Run("urlencoded", func(t *testing.T) {
		compare(t, h, "POST", "/bind/path?id=query&name=qs&tag=qtag", "application/x-www-form-urlencoded", "id=body&name=form&tag=ftag", "id=body name=form tag=ftag")
	})
	t.Run("multipart", func(t *testing.T) {
		ct, b := multipartBody(t, [][2]string{{"note", "body"}}, []upload{{"upload", "test.txt", "hello"}})
		compare(t, h, "POST", "/upload?note=query", ct, b, "note=body file=test.txt")
	})
}
func TestGinBindCompatibility(t *testing.T)  { bindCases(t, ginHandler()) }
func TestEchoBindCompatibility(t *testing.T) { bindCases(t, echoHandler()) }
func gapCases(t *testing.T, h http.Handler, gin bool) {
	t.Run("ParseForm_and_FormValue", func(t *testing.T) {
		want := "name=hon raw=hon"
		if gin {
			want = "name=hon formValue=hon raw=hon"
		}
		compare(t, h, "POST", "/form", "application/x-www-form-urlencoded", "name=hon", want)
	})
	t.Run("MultipartForm_and_FormFile", func(t *testing.T) {
		ct, b := multipartBody(t, [][2]string{{"note", "hon"}}, []upload{{"upload", "test.txt", "hello"}})
		compare(t, h, "POST", "/multipart", ct, b, "note=hon file=test.txt count=1")
	})
}
func TestGinCompatibilityGaps(t *testing.T)  { gapCases(t, ginHandler(), true) }
func TestEchoCompatibilityGaps(t *testing.T) { gapCases(t, echoHandler(), false) }
