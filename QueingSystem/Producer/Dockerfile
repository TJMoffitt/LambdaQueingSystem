FROM public.ecr.aws/lambda/python@sha256:2203844d7263f9c38a7f03f25bbb181772e93abc944de2b26ccca1689e1dde55 as build

FROM public.ecr.aws/lambda/python@sha256:2203844d7263f9c38a7f03f25bbb181772e93abc944de2b26ccca1689e1dde55
RUN yum install atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel -y

COPY requirements.txt .
RUN pip install --upgrade pip -q
RUN pip install -r requirements.txt


# Copy function code
COPY /* ${FUNCTION_DIR}

# Copy handler function
COPY app/* ${FUNCTION_DIR}

CMD [ "app.handler" ]
