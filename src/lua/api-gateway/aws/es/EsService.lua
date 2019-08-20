local AwsService = require "api-gateway.aws.AwsService"

local _M = AwsService:new({ ___super = true })
local super = {
    instance = _M,
    constructor = _M.constructor
}

function _M:new(o)
    ngx.log(ngx.DEBUG, "EsService() o=", tostring(o))
    local o = o or {}
    o.aws_service = "es"
    -- aws_service_name is used in the X-Amz-Target Header: i.e ES
    o.aws_service_name = "ES"
    self.es_endpoint = o.es_endpoint

    super.constructor(_M, o)

    setmetatable(o, self)
    self.__index = self
    return o
end

function _M:getAWSHost()
    return self.es_endpoint
end

local function url_encode(str)
  if (str) then
    str = string.gsub (str, "\n", "\r\n")
    str = string.gsub (str, "%:", "%%3A")
    str = string.gsub (str, "%*", "%%2A")
    str = string.gsub (str, " ", "+")
  end
  return str
end

function _M:setHeaders()
  local request_method = ngx.var.request_method
  local request_query_string = ngx.req.get_uri_args()
  local extra_headers = ngx.req.get_headers()

  ngx.log(ngx.DEBUG, "PATH:")
  ngx.log(ngx.DEBUG, ngx.var.uri)

  -- TODO: we could optimize this to run only for POST requests
  -- Try to read in request body
  ngx.req.read_body()
  -- Try to load request body data to a variable
  local request_body = ngx.req.get_body_data()
  ngx.log(ngx.DEBUG, "BODY:")
  ngx.log(ngx.DEBUG, request_body)
  if not request_body then
    -- If empty, try to get buffered file name
    local request_body_file_name = ngx.req.get_body_file()
    -- If the file has been buffered, open it, read contents to our variable and close
    if request_body_file_name then
        file = io.open(request_body_file_name)
        request_body = file:read("*a")
        file:close()
    end
  end
  ngx.log(ngx.DEBUG, request_body)

  local authorization, awsAuth, authToken, payloadHash = self:getAuthorizationHeader(request_method, url_encode(ngx.var.uri), request_query_string, request_body)
  local request_headers = {
      ["Authorization"] = authorization,
      ["X-Amz-Date"] = awsAuth.aws_date,
      ["Accept"] = "application/json",
      ["x-amz-security-token"] = authToken,
      ["x-amz-content-sha256"] = payloadHash
  }
  if ( extra_headers ~= nil ) then
      for headerName, headerValue in pairs(extra_headers) do
          request_headers[headerName] = headerValue
      end
  end

  for headerName, headerValue in pairs(request_headers) do
    ngx.req.set_header(headerName, headerValue)
  end
end

return _M
