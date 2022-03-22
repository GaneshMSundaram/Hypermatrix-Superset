from flask_appbuilder.security.views import expose
from flask_appbuilder.security.sqla.manager import SecurityManager
from flask_appbuilder.security.manager import BaseSecurityManager
from flask_appbuilder.security.manager import AUTH_REMOTE_USER
from flask_login import login_user
from flask import redirect, g, flash, request
from superset.security.manager import SupersetSecurityManager

# Create a custom view to authenticate the user
AuthRemoteUserView=BaseSecurityManager.authremoteuserview
class SupersetAuthRemoteUserView(AuthRemoteUserView):
    @expose('/login/')
    def login(self):
      print("This is LOGIN")
      user = self.appbuilder.sm.find_user('admin')
      login_user(user, remember=False)

      # Can also get redirect url after parsing url 
      redirect_url = '/superset/welcome/'
      return redirect(redirect_url)


# Create a custom Security manager that override the authremoteuserview above
class CustomSecurityManagerSameFile(SupersetSecurityManager):
    authremoteuserview = SupersetAuthRemoteUserView