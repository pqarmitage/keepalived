/*
 * Soft:        Keepalived is a failover program for the LVS project
 *              <www.linuxvirtualserver.org>. It monitor & manipulate
 *              a loadbalanced server pool using multi-layer checks.
 *
 * Part:        DBus server thread for VRRP
 *
 * Author:      Alexandre Cassen, <acassen@linux-vs.org>
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *              See the GNU General Public License for more details.
 *
 *              This program is free software; you can redistribute it and/or
 *              modify it under the terms of the GNU General Public License
 *              as published by the Free Software Foundation; either version
 *              2 of the License, or (at your option) any later version.
 *
 * Copyright (C) 2001-2012 Alexandre Cassen, <acassen@gmail.com>
 */

#include "vrrp_dbus.h"
#include "vrrp_data.h"
#include "vrrp_if.h"
#include "vrrp_print.h"
#include "logger.h"

#ifdef _WITH_DBUS_
#include <gio/gio.h>
/* Global file variables */

static GDBusNodeInfo *vrrp_introspection_data = NULL;
static GDBusNodeInfo *vrrp_instance_introspection_data = NULL;
static GDBusConnection *global_connection;
static GSList *objects = NULL;
static GMainLoop *loop;

/* send signal VrrpStatusChange
 * containing the new state of vrrp */
void
dbus_send_state_signal(vrrp_t *vrrp)
{
	GError **local_error;

	gchar *object_path = g_strconcat(DBUS_VRRP_INSTANCE_OBJECT_ROOT,
								IF_NAME(vrrp->ifp), "/", g_strdup_printf("%d", vrrp->vrid),  NULL);
	GVariant *args = g_variant_new("(u)", vrrp->state);

	/* the interface will go through the initial state changes before
	 * the main loop can be started and global_connection initialised */
	if (global_connection == NULL) {
		log_message(LOG_INFO, "Not connected to the org.keepalived.Vrrp1 bus");
	} else {
		g_dbus_connection_emit_signal (global_connection, NULL, object_path,
										DBUS_VRRP_INSTANCE_INTERFACE,
										"VrrpStatusChange", args, local_error);
	}
	g_free(object_path);
}

/* handles reply to org.freedesktop.DBus.Properties.Get method on any object*/
static GVariant *
handle_get_property (GDBusConnection  *connection,
					 const gchar      *sender,
					 const gchar      *object_path,
					 const gchar      *interface_name,
					 const gchar      *property_name,
					 GError          **error,
					 gpointer          user_data)
{
	GVariant *ret;

	if (g_strcmp0 (interface_name, DBUS_VRRP_INSTANCE_INTERFACE) == 0) {

		/* object_path will be in the form /org/keepalived/Vrrp1/Instance/INTERFACE/GROUP */
		gchar **dirs = g_strsplit(object_path, "/", 7);
		gchar *interface = dirs[5];
		gchar *group = dirs[6];
		/* we look for the vrrp instance object that corresponds to our interface and group */
		list l = vrrp_data->vrrp;
		element e;
		for (e = LIST_HEAD(l); e; ELEMENT_NEXT(e)) {
			vrrp_t * vrrp = ELEMENT_DATA(e);
			gchar *vrrp_vrid =  g_strdup_printf("%d", vrrp->vrid);
			if (g_strcmp0 (interface, IF_NAME(vrrp->ifp)) == 0
				&& g_strcmp0 (group, vrrp_vrid) == 0 ) {
				/* the property_name argument is the property we want to Get */
				if (g_strcmp0 (property_name, "Name") == 0)
					ret = g_variant_new("(s)", vrrp->iname);
				 else if (g_strcmp0 (property_name, "State") == 0)
					ret = g_variant_new("(u)", vrrp->state);
				 else
					log_message(LOG_INFO, "This property does not exist");
			 	 break;
			}
		}

	} else {
		log_message(LOG_INFO, "This interfce has not been implemented yet");
	}

	return ret;
}

/* handles method_calls on any object */
static void
handle_method_call (GDBusConnection       *connection,
					const gchar           *sender,
					const gchar           *object_path,
					const gchar           *interface_name,
					const gchar           *method_name,
					GVariant              *parameters,
					GDBusMethodInvocation *invocation,
					gpointer               user_data)
{
	if (g_strcmp0 (interface_name, DBUS_VRRP_INTERFACE) == 0) {

		if (g_strcmp0 (method_name, "PrintData") == 0) {
			log_message(LOG_INFO, "Printing VRRP data for process(%d) on signal",
				getpid());
			vrrp_print_data();
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0 (method_name, "PrintStats") == 0) {
			log_message(LOG_INFO, "Printing VRRP stats for process(%d) on signal",
				getpid());
			vrrp_print_stats();
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else {
			log_message(LOG_INFO, "This method has not been implemented yet");
		}

	} else if (g_strcmp0 (interface_name, DBUS_VRRP_INSTANCE_INTERFACE) == 0) {

		if (g_strcmp0 (method_name, "SendGarp") == 0) {
			GVariant *name_call =  handle_get_property(connection, sender, object_path,
												  interface_name, "Name", NULL, NULL);
			gchar *name;
			g_variant_get (name_call, "(&s)", &name);

			list l = vrrp_data->vrrp;
			element e;
			for (e = LIST_HEAD(l); e; ELEMENT_NEXT(e)) {
				vrrp_t * vrrp = ELEMENT_DATA(e);
				if (g_strcmp0 (vrrp->iname, name) == 0) {
				    vrrp_send_link_update(vrrp, 1);
					g_dbus_method_invocation_return_value(invocation, NULL);
					break;
				}
			}
		} else {
			log_message(LOG_INFO, "This method has not been implemented yet");
		}

	} else {
		log_message(LOG_INFO, "This interfce has not been implemented yet");
	}

}

static const GDBusInterfaceVTable interface_vtable =
{
	handle_method_call,
	handle_get_property,
	NULL /* handle_set_property is null because we have no writeable property */
};

/* first function to be run when trying to own bus,
 * exports objects to the bus */
static void
on_bus_acquired (GDBusConnection *connection,
				 const gchar     *name,
				 gpointer         user_data)
{
	global_connection = connection;

	/* register VRRP object */
	guint vrrp = g_dbus_connection_register_object(connection, DBUS_VRRP_OBJECT,
												 vrrp_introspection_data->interfaces[0],
												 &interface_vtable, NULL, NULL, NULL);
	objects = g_slist_append(objects, GUINT_TO_POINTER(vrrp));

	/* for each available VRRP instance, register an object */
	list l = vrrp_data->vrrp;
	element e;
	guint instance;
	for (e = LIST_HEAD(l); e; ELEMENT_NEXT(e)) {
		vrrp_t * vrrp = ELEMENT_DATA(e);
		gchar *vrid = g_strdup_printf("/%d", vrrp->vrid);
		gchar *path = g_strconcat(DBUS_VRRP_INSTANCE_OBJECT_ROOT, IF_NAME(vrrp->ifp), vrid, NULL);
		instance = g_dbus_connection_register_object(connection, path,
													 vrrp_instance_introspection_data->interfaces[0],
												 	&interface_vtable, NULL, NULL, NULL);
		if (instance != 0)
			objects = g_slist_append(objects, GUINT_TO_POINTER(instance));

		g_free(path);
		g_free(vrid);
	}
}

/* run if bus name is acquired successfully */
static void
on_name_acquired (GDBusConnection *connection,
				  const gchar     *name,
				  gpointer         user_data)
{
	log_message(LOG_INFO, "Acquired the name %s on the session bus\n", name);
}

static void
unregister_object(gpointer data, gpointer user_data)
{
	guint *object = (guint *) data;
	g_dbus_connection_unregister_object(global_connection, &object);
}

/* run if bus name or connection are lost */
static void
on_name_lost (GDBusConnection *connection,
			  const gchar     *name,
			  gpointer         user_data)
{
	log_message(LOG_INFO, "Lost the name %s on the session bus\n", name);
	global_connection = connection;
	g_slist_foreach(objects, unregister_object, NULL);
	objects = NULL;
	global_connection = NULL;
}

static gchar*
read_file(gchar* filepath)
{
	FILE * f;
	long length;
	gchar *ret;
	f = fopen (filepath, "rb");
	if (f) {
		fseek (f, 0, SEEK_END);
		length = ftell (f);
		fseek (f, 0, SEEK_SET);
		ret = malloc (length);
		if (ret)
			fread (ret, 1, length, f);
		fclose (f);
	}
	return ret;
}

void
dbus_stop()
{
	GError **local_error;
	if (global_connection != NULL)
			g_dbus_connection_emit_signal (global_connection, NULL, DBUS_VRRP_OBJECT,
				   DBUS_VRRP_INTERFACE, "VrrpStopped", NULL, local_error);
	g_main_loop_quit(loop);
}

void
dbus_main ()
{
	gchar *introspection_xml;
	guint owner_id;

	/* DBus service org.keepalived.Vrrp1 exposes two interfaces, Vrrp and Instance.
	 * Vrrp is implemented by a single Vrrp object for general purposes, such as printing
	 * data or signaling that the Vrrp process has been stopped.
	 * Instance is implemented by an Instance object for every VRRP Instance in vrrp_data.
	 * It exposes instance specific methods and properties.
	 */

	/* read service interface data from xml files */
	introspection_xml = read_file(DBUS_VRRP_INTERFACE_FILE_PATH);
	vrrp_introspection_data = g_dbus_node_info_new_for_xml (introspection_xml, NULL);
	free(introspection_xml);
	introspection_xml = NULL;

	introspection_xml = read_file(DBUS_VRRP_INSTANCE_INTERFACE_FILE_PATH);
	vrrp_instance_introspection_data = g_dbus_node_info_new_for_xml (introspection_xml, NULL);
	free(introspection_xml);

	owner_id = g_bus_own_name (G_BUS_TYPE_SYSTEM,
							   DBUS_SERVICE_NAME,
							   G_BUS_NAME_OWNER_FLAGS_NONE,
							   on_bus_acquired,
							   on_name_acquired,
							   on_name_lost,
							   NULL,  /* user_data */
							   NULL); /* user_data_free_func */

	loop = g_main_loop_new (NULL, FALSE);
	g_main_loop_run (loop);

	/* cleanup after loop terminates */
	g_bus_unown_name (owner_id);
	global_connection = NULL;
	pthread_exit(0);
}

#else // _WITH_DBUS_

void
dbus_send_state_signal(vrrp_t *vrrp){}

void
dbus_stop(){}

void
dbus_main()
{
	pthread_exit(0);
}
#endif // _WITH_DBUS_
