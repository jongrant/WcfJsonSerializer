using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.ServiceModel.Channels;
using System.ServiceModel.Configuration;
using System.ServiceModel.Description;
using System.ServiceModel.Dispatcher;
using System.ServiceModel.Web;
using System.Text;
using System.Xml;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace JonGrant.Json
{
    // Based on http://itq.nl/replacing-wcf-datacontractjsonserializer-with-newtonsoft-jsonserializer/
    // See http://jongrant.org/2016/07/12/microsofts-wcf-datacontractjsonserializer-sucks/

    public class NewtonsoftJsonDispatchFormatter : IDispatchMessageFormatter
    {
        OperationDescription operation;
        Dictionary<string, int> parameterNames;

        public NewtonsoftJsonDispatchFormatter(OperationDescription operation, bool isRequest)
        {
            this.operation = operation;
            if (isRequest)
            {
                int operationParameterCount = operation.Messages[0].Body.Parts.Count;
                if (operationParameterCount > 1)
                {
                    this.parameterNames = new Dictionary<string, int>();
                    for (int i = 0; i < operationParameterCount; i++)
                    {
                        this.parameterNames.Add(operation.Messages[0].Body.Parts[i].Name, i);
                    }
                }
            }
        }

        public void DeserializeRequest(Message message, object[] parameters)
        {
            object bodyFormatProperty;
            if (!message.Properties.TryGetValue(WebBodyFormatMessageProperty.Name, out bodyFormatProperty) ||
                (bodyFormatProperty as WebBodyFormatMessageProperty).Format != WebContentFormat.Raw)
            {
                throw new InvalidOperationException("Incoming messages must have a body format of Raw. Is a ContentTypeMapper set on the WebHttpBinding?");
            }

            var bodyReader = message.GetReaderAtBodyContents();
            bodyReader.ReadStartElement("Binary");
            byte[] rawBody = bodyReader.ReadContentAsBase64();
            var ms = new MemoryStream(rawBody);

            var sr = new StreamReader(ms);
            var serializer = new Newtonsoft.Json.JsonSerializer();
            serializer.Converters.Add(new StringEnumConverter { AllowIntegerValues = false, CamelCaseText = false });
            if (parameters.Length == 1)
            {
                // single parameter, assuming bare
                parameters[0] = serializer.Deserialize(sr, operation.Messages[0].Body.Parts[0].Type);
            }
            else
            {
                // multiple parameter, needs to be wrapped
                Newtonsoft.Json.JsonReader reader = new Newtonsoft.Json.JsonTextReader(sr);
                reader.Read();
                if (reader.TokenType != Newtonsoft.Json.JsonToken.StartObject)
                {
                    throw new InvalidOperationException("Input needs to be wrapped in an object");
                }

                reader.Read();
                while (reader.TokenType == Newtonsoft.Json.JsonToken.PropertyName)
                {
                    var parameterName = reader.Value as string;
                    reader.Read();
                    if (this.parameterNames.ContainsKey(parameterName))
                    {
                        int parameterIndex = this.parameterNames[parameterName];
                        parameters[parameterIndex] = serializer.Deserialize(reader, this.operation.Messages[0].Body.Parts[parameterIndex].Type);
                    }
                    else
                    {
                        reader.Skip();
                    }

                    reader.Read();
                }

                reader.Close();
            }

            sr.Close();
            ms.Close();
        }

        public Message SerializeReply(MessageVersion messageVersion, object[] parameters, object result)
        {
            return FormatObjectAsMessage(result, messageVersion, operation.Messages[1].Action, HttpStatusCode.OK);
        }

        public static Message FormatObjectAsMessage(object obj, MessageVersion messageVersion, string action, HttpStatusCode statusCode)
        {
            byte[] body;
            var serializer = new JsonSerializer();
            serializer.Converters.Add(new StringEnumConverter { AllowIntegerValues = false, CamelCaseText = false });

            using (var ms = new MemoryStream())
            {
                using (var sw = new StreamWriter(ms, Encoding.UTF8))
                {
                    using (JsonWriter writer = new Newtonsoft.Json.JsonTextWriter(sw))
                    {
                        //writer.Formatting = Newtonsoft.Json.Formatting.Indented;
                        serializer.Serialize(writer, obj);
                        sw.Flush();
                        body = ms.ToArray();
                    }
                }
            }

            Message replyMessage = Message.CreateMessage(messageVersion, action, new RawBodyWriter(body));
            replyMessage.Properties.Add(WebBodyFormatMessageProperty.Name, new WebBodyFormatMessageProperty(WebContentFormat.Raw));
            var respProp = new HttpResponseMessageProperty();
            respProp.Headers[HttpResponseHeader.ContentType] = "application/json";
            respProp.StatusCode = statusCode;
            replyMessage.Properties.Add(HttpResponseMessageProperty.Name, respProp);
            return replyMessage;
        }
    }

    public class RawBodyWriter : BodyWriter
    {
        byte[] content;
        public RawBodyWriter(byte[] content)
            : base(true)
        {
            this.content = content;
        }

        protected override void OnWriteBodyContents(XmlDictionaryWriter writer)
        {
            writer.WriteStartElement("Binary");
            writer.WriteBase64(content, 0, content.Length);
            writer.WriteEndElement();
        }
    }

    public class NewtonsoftJsonBehavior : WebHttpBehavior
    {
        private bool includeExceptionDetailInFaults = false;

        public NewtonsoftJsonBehavior(bool includeExceptionDetailInFaults)
        {
            this.includeExceptionDetailInFaults = includeExceptionDetailInFaults;
        }

        public override void Validate(ServiceEndpoint endpoint)
        {
            base.Validate(endpoint);

            var elements = endpoint.Binding.CreateBindingElements();
            var webEncoder = elements.Find<WebMessageEncodingBindingElement>();
            if (webEncoder == null)
            {
                throw new InvalidOperationException("This behavior must be used in an endpoint with the WebHttpBinding (or a custom binding with the WebMessageEncodingBindingElement).");
            }

            foreach (OperationDescription operation in endpoint.Contract.Operations)
            {
                this.ValidateOperation(operation);
            }
        }

        protected override IDispatchMessageFormatter GetRequestDispatchFormatter(OperationDescription operationDescription, ServiceEndpoint endpoint)
        {
            if (this.IsGetOperation(operationDescription))
            {
                // no change for GET operations
                return base.GetRequestDispatchFormatter(operationDescription, endpoint);
            }

            if (operationDescription.Messages[0].Body.Parts.Count == 0)
            {
                // nothing in the body, still use the default
                return base.GetRequestDispatchFormatter(operationDescription, endpoint);
            }

            return new NewtonsoftJsonDispatchFormatter(operationDescription, true);
        }

        protected override IDispatchMessageFormatter GetReplyDispatchFormatter(OperationDescription operationDescription, ServiceEndpoint endpoint)
        {
            if (operationDescription.Messages.Count == 1 || operationDescription.Messages[1].Body.ReturnValue.Type == typeof(void))
            {
                return base.GetReplyDispatchFormatter(operationDescription, endpoint);
            }
            else
            {
                return new NewtonsoftJsonDispatchFormatter(operationDescription, false);
            }
        }

        private void ValidateOperation(OperationDescription operation)
        {
            if (operation.Messages.Count > 1)
            {
                if (operation.Messages[1].Body.Parts.Count > 0)
                {
                    throw new InvalidOperationException("Operations cannot have out/ref parameters.");
                }
            }

            WebMessageBodyStyle bodyStyle = this.GetBodyStyle(operation);
            int inputParameterCount = operation.Messages[0].Body.Parts.Count;
            if (!this.IsGetOperation(operation))
            {
                var wrappedRequest = bodyStyle == WebMessageBodyStyle.Wrapped || bodyStyle == WebMessageBodyStyle.WrappedRequest;
                if (inputParameterCount == 1 && wrappedRequest)
                {
                    throw new InvalidOperationException("Wrapped body style for single parameters not implemented in this behavior.");
                }
            }

            var wrappedResponse = bodyStyle == WebMessageBodyStyle.Wrapped || bodyStyle == WebMessageBodyStyle.WrappedResponse;
            var isVoidReturn = operation.Messages.Count == 1 || operation.Messages[1].Body.ReturnValue.Type == typeof(void);
            if (!isVoidReturn && wrappedResponse)
            {
                throw new InvalidOperationException("Wrapped response not implemented in this behavior.");
            }
        }

        private WebMessageBodyStyle GetBodyStyle(OperationDescription operation)
        {
            var wga = operation.Behaviors.Find<WebGetAttribute>();
            if (wga != null)
            {
                return wga.BodyStyle;
            }

            var wia = operation.Behaviors.Find<WebInvokeAttribute>();
            if (wia != null)
            {
                return wia.BodyStyle;
            }

            return this.DefaultBodyStyle;
        }

        private bool IsGetOperation(OperationDescription operation)
        {
            var wga = operation.Behaviors.Find<WebGetAttribute>();
            if (wga != null)
            {
                return true;
            }

            var wia = operation.Behaviors.Find<WebInvokeAttribute>();
            if (wia != null)
            {
                return wia.Method == "HEAD";
            }

            return false;
        }

        public override void ApplyDispatchBehavior(ServiceEndpoint endpoint, EndpointDispatcher endpointDispatcher)
        {
            base.ApplyDispatchBehavior(endpoint, endpointDispatcher);
            
            endpointDispatcher.ChannelDispatcher.ErrorHandlers.Clear();
            endpointDispatcher.ChannelDispatcher.ErrorHandlers.Add(new NewtonsoftJsonErrorHandler(includeExceptionDetailInFaults));
        }
    }

    [DataContract(Name = "error")]
    public class JsonErrorMessage
    {
        [DataMember(Name = "message")]
        public string Message
        {
            get;
            set;
        }

        [DataMember(Name = "stackTrace", EmitDefaultValue = false)]
        public string StackTrace
        {
            get;
            set;
        }

        [DataMember(Name = "inner", EmitDefaultValue = false)]
        public JsonErrorMessage Inner
        {
            get;
            set;
        }

        public JsonErrorMessage(string message)
        {
            this.Message = message;
        }

        public JsonErrorMessage(string message, string stackTrace)
        {
            this.Message = message;
            this.StackTrace = stackTrace;
        }

        public static JsonErrorMessage FromException(Exception ex, bool includeExceptionDetailInFaults)
        {
            JsonErrorMessage result = null;

            if (includeExceptionDetailInFaults)
            {
                result = new JsonErrorMessage(ex.Message, ex.StackTrace);
                if (ex.InnerException != null) result.Inner = FromException(ex.InnerException, includeExceptionDetailInFaults);
            }
            else
            {
                result = new JsonErrorMessage(ex.Message);
            }

            return result;
        }
    }

    public class NewtonsoftJsonErrorHandler : IErrorHandler
    {
        private bool includeExceptionDetailInFaults = false;

        public NewtonsoftJsonErrorHandler(bool includeExceptionDetailInFaults)
        {
            this.includeExceptionDetailInFaults = includeExceptionDetailInFaults;
        }

        public bool HandleError(Exception error)
        {
            return false;
        }

        public void ProvideFault(Exception error, MessageVersion version, ref Message fault)
        {
            var wrapped = new { error = JsonErrorMessage.FromException(error, includeExceptionDetailInFaults) };

            var statusCode = HttpStatusCode.InternalServerError;
            if (error is WebFaultException) statusCode = ((WebFaultException)error).StatusCode;
            
            fault = NewtonsoftJsonDispatchFormatter.FormatObjectAsMessage(wrapped, version, "fault", statusCode);
        }
    }

    public class NewtonsoftJsonBehaviorExtension : BehaviorExtensionElement
    {
        [ConfigurationProperty("includeExceptionDetailInFaults")]
        public bool IncludeExceptionDetailInFaults
        {
            get { return Boolean.Parse(this["includeExceptionDetailInFaults"].ToString()); }
            set { this["includeExceptionDetailInFaults"] = value; }
        }

        public override Type BehaviorType
        {
            get { return typeof(NewtonsoftJsonBehavior); }
        }

        protected override object CreateBehavior()
        {
            return new NewtonsoftJsonBehavior(this.IncludeExceptionDetailInFaults);
        }
    }

    public class NewtonsoftJsonContentTypeMapper : WebContentTypeMapper
    {
        public override WebContentFormat GetMessageFormatForContentType(string contentType)
        {
            return WebContentFormat.Raw;
        }
    }
}
