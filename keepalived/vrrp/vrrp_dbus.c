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
 * Copyright (C) 2016-2016 Alexandre Cassen, <acassen@gmail.com>
 */

#include "config.h"

#include <pthread.h>
#include <gio/gio.h>
#include <ctype.h>
#include <fcntl.h>
#include <unistd.h>

#include "vrrp_dbus.h"
#include "vrrp_data.h"
#include "vrrp_if.h"
#include "vrrp_print.h"
#include "main.h"
#include "memory.h"
#include "logger.h"
#include "timer.h"
#include "scheduler.h"
#include "list.h"
#if HAVE_DECL_CLONE_NEWNET
#include "namespaces.h"
#endif

typedef enum dbus_action {
	DBUS_ACTION_NONE,
	DBUS_PRINT_DATA,
	DBUS_PRINT_STATS,
	DBUS_CREATE_INSTANCE,
	DBUS_DESTROY_INSTANCE,
	DBUS_SEND_GARP,
	DBUS_GET_NAME,
	DBUS_GET_STATUS,
} dbus_action_t;

typedef enum dbus_error {
	DBUS_SUCCESS,
	DBUS_INTERFACE_NOT_FOUND,
	DBUS_OBJECT_ALREADY_EXISTS,
} dbus_error_t;

typedef struct dbus_queue_ent {
	dbus_action_t action;
	dbus_error_t reply;
	char str[IFNAMSIZ+1];
	int val;
	GVariant *args;
} dbus_queue_ent_t;

/* Global file variables */
static GDBusNodeInfo *vrrp_introspection_data = NULL;
static GDBusNodeInfo *vrrp_instance_introspection_data = NULL;
static GDBusConnection *global_connection;
static GHashTable *objects;
static GMainLoop *loop;

/* Queues between main vrrp thread and dbus thread */
static list dbus_in_queue, dbus_out_queue;
static pthread_mutex_t in_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t out_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static int dbus_in_pipe[2], dbus_out_pipe[2];

/* The only characters that are valid in a dbus path are A-Z, a-z, 0-9, _ */
static char *
set_valid_path(char *valid_path, const char *path)
{
	const char *str_in;
	char *str_out;

	for (str_in = path, str_out = valid_path; *str_in; str_in++, str_out++) {
		if (!isalnum(*str_in) && *str_in != '_')
			*str_out = '_';
		else
			*str_out = *str_in;
	}
	*str_out = '\0';

	return valid_path;
}

static gboolean
unregister_object(gpointer key, gpointer value, gpointer user_data)
{
	guint object = GPOINTER_TO_UINT(value);
	return g_dbus_connection_unregister_object(global_connection, object);
}

static gchar *
dbus_object_create_path_vrrp(void)
{
	gchar *object_path = DBUS_VRRP_OBJECT_ROOT;

#ifdef HAVE_DECL_CLONE_NEWNET
	if(network_namespace != NULL)
		object_path = g_strconcat(object_path, "/", network_namespace, NULL);
#endif
	if(instance_name)
		object_path = g_strconcat(object_path, "/", instance_name, NULL);

	object_path = g_strconcat(object_path, "/Vrrp", NULL);
	return object_path;
}

static gchar *
dbus_object_create_path_instance(gchar *interface, gchar *group)
{
	gchar *object_path = DBUS_VRRP_OBJECT_ROOT;
	char standardized_name[sizeof ((vrrp_t*)NULL)->ifp->ifname];

#ifdef HAVE_DECL_CLONE_NEWNET
	if(network_namespace != NULL)
		object_path = g_strconcat(object_path, "/", network_namespace, NULL);
#endif
	if(instance_name)
		object_path = g_strconcat(object_path, "/", instance_name, NULL);

	object_path = g_strconcat(object_path, "/Instance/",
				set_valid_path(standardized_name, interface), "/", group,  NULL);
	return object_path;
}

static dbus_queue_ent_t *
process_method_call(dbus_action_t action, GVariant *args, bool return_data)
{
	dbus_queue_ent_t *ent = MALLOC(sizeof(dbus_queue_ent_t));
	element e;

	if (!ent)
		return NULL;

	ent->action = action;
	pthread_mutex_lock(&in_queue_lock);

	char *param = NULL;
	int val = 0;

	if (args){
		if (g_variant_is_of_type(args, G_VARIANT_TYPE("(su)")))
			g_variant_get(args, "(su)", &param, &val);
		else if (g_variant_is_of_type(args, G_VARIANT_TYPE("(s)")))
			g_variant_get(args, "(s)", &param);
		else if (g_variant_is_of_type(args, G_VARIANT_TYPE("(ssu)"))){
			char *iname;
			g_variant_get(args, "(ssu)", &iname, &param, &val);
			ent->args = g_variant_new("(s)", iname);
		}
	}

	if (param)
		strcpy(ent->str, param);
	ent->val = val;
	list_add(dbus_in_queue, ent);
	pthread_mutex_unlock(&in_queue_lock);

	/* Tell the main thread that a queue entry is waiting. Any data works */
	write(dbus_in_pipe[1], ent, 1);

	/* Wait for a response */
	while (read(dbus_out_pipe[0], ent, 1) == -1 && errno == EINTR) {
		log_message(LOG_INFO, "dbus_out_pipe read returned EINTR");
	}

	/* We could loop through looking for e->data == ent */
	pthread_mutex_lock(&out_queue_lock);
	if (!LIST_ISEMPTY(dbus_out_queue)) {
		e = LIST_HEAD(dbus_out_queue);
		free_list_element(dbus_out_queue, e);
		if (ent != e->data)
			log_message(LOG_INFO, "Returned dbus entry mismatch");
	}
	else
		log_message(LOG_INFO, "Empty dbus out queue");
	pthread_mutex_unlock(&out_queue_lock);

	if (ent->action != action)
		log_message(LOG_INFO, "DBus expected receive action %d and received %d", action, ent->action);
	if (ent->reply != DBUS_SUCCESS) {
		if (ent->reply == DBUS_INTERFACE_NOT_FOUND)
			log_message(LOG_INFO, "Unable to find DBus requested interface %s/%d", param, val);
		else if (ent-> reply == DBUS_OBJECT_ALREADY_EXISTS)
			log_message(LOG_INFO, "Unable to create DBus requested object with interface %s/%d", param, val);
		else
			log_message(LOG_INFO, "Unknown DBus reply %d", ent->reply);
	}

	if (!return_data) {
		FREE(ent);
		ent = NULL;
	}

	return ent;
}

/* handles reply to org.freedesktop.DBus.Properties.Get method on any object*/
static GVariant *
handle_get_property(GDBusConnection  *connection,
		    const gchar      *sender,
		    const gchar      *object_path,
		    const gchar      *interface_name,
		    const gchar      *property_name,
		    GError          **error,
		    gpointer          user_data)
{
	GVariant *ret = NULL;
	dbus_queue_ent_t *ent;

	if (!g_strcmp0(interface_name, DBUS_VRRP_INSTANCE_INTERFACE)) {

		int path_length = DBUS_VRRP_INSTANCE_PATH_DEFAULT_LENGTH;
#ifdef HAVE_DECL_CLONE_NEWNET
		if(network_namespace != NULL)
			path_length++;
#endif
		if(instance_name)
			path_length++;

		/* object_path will have interface and group as the two last levels */
		gchar **dirs = g_strsplit(object_path, "/", path_length);
		gchar *interface = dirs[path_length-2];
		unsigned vrid = atoi(dirs[path_length-1]);
		int action = DBUS_ACTION_NONE;

		if (!g_strcmp0(property_name, "Name"))
			action = DBUS_GET_NAME;
		else if (!g_strcmp0(property_name, "State"))
			action = DBUS_GET_STATUS;
		else
			log_message(LOG_INFO, "Property %s does not exist", property_name);

		if (action != DBUS_ACTION_NONE) {
			GVariant *args = g_variant_new("(su)", interface, vrid);
			ent = process_method_call(action, args, true);

			if (ent) {
				if (ent->reply == DBUS_SUCCESS) {
					if (action == DBUS_GET_NAME)
						ret = g_variant_new("(s)", ent->str);
					else if (action == DBUS_GET_STATUS)
						ret = g_variant_new("(u)", ent->val);
				}

				FREE(ent);
			}
		}
	} else
		log_message(LOG_INFO, "Interface %s has not been implemented yet", interface_name);

	return ret;
}

/* handles method_calls on any object */
static void
handle_method_call(GDBusConnection *connection,
		   const gchar           *sender,
		   const gchar           *object_path,
		   const gchar           *interface_name,
		   const gchar           *method_name,
		   GVariant              *parameters,
		   GDBusMethodInvocation *invocation,
		   gpointer               user_data)
{
	if (!g_strcmp0(interface_name, DBUS_VRRP_INTERFACE)) {
		if (!g_strcmp0(method_name, "PrintData")) {
			process_method_call(DBUS_PRINT_DATA, NULL, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0(method_name, "PrintStats") == 0) {
			process_method_call(DBUS_PRINT_STATS, NULL, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0(method_name, "CreateInstance") == 0) {
			process_method_call(DBUS_CREATE_INSTANCE, parameters, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0(method_name, "DestroyInstance") == 0) {
			process_method_call(DBUS_DESTROY_INSTANCE, parameters, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else
			log_message(LOG_INFO, "Method %s has not been implemented yet", method_name);
	} else if (!g_strcmp0(interface_name, DBUS_VRRP_INSTANCE_INTERFACE)) {
		if (!g_strcmp0(method_name, "SendGarp")) {
			GVariant *name_call =  handle_get_property(connection, sender, object_path,
								   interface_name, "Name", NULL, NULL);
			if (!name_call)
				log_message(LOG_INFO, "Name property not found");
			else {
				process_method_call(DBUS_SEND_GARP, name_call, false);
				g_dbus_method_invocation_return_value(invocation, NULL);
			}
		} else
			log_message(LOG_INFO, "Method %s has not been implemented yet", method_name);
	} else
		log_message(LOG_INFO, "Interface %s has not been implemented yet", interface_name);
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
on_bus_acquired(GDBusConnection *connection,
		const gchar     *name,
		gpointer         user_data)
{
	global_connection = connection;

	log_message(LOG_INFO, "Acquired DBus bus %s\n", name);

	/* register VRRP object */
	guint vrrp = g_dbus_connection_register_object(connection, dbus_object_create_path_vrrp(),
												 vrrp_introspection_data->interfaces[0],
												 &interface_vtable, NULL, NULL, NULL);
	g_hash_table_insert(objects, "__Vrrp__", GUINT_TO_POINTER(vrrp));
	
	/* for each available VRRP instance, register an object */
	list l = vrrp_data->vrrp;
	if (LIST_ISEMPTY(l))
		return;

	element e;
	guint instance;
	for (e = LIST_HEAD(l); e; ELEMENT_NEXT(e)) {
		vrrp_t * vrrp = ELEMENT_DATA(e);

		gchar *path = dbus_object_create_path_instance(IF_NAME(IF_BASE_IFP(vrrp->ifp)),
						g_strdup_printf("%d",vrrp->vrid));
		instance = g_dbus_connection_register_object(connection, path,
							     vrrp_instance_introspection_data->interfaces[0],
							     &interface_vtable, NULL, NULL, NULL);
		if (instance != 0)
			g_hash_table_insert(objects, vrrp->iname, GUINT_TO_POINTER(instance));

		g_free(path);
	}
}

/* run if bus name is acquired successfully */
static void
on_name_acquired(GDBusConnection *connection,
		 const gchar     *name,
		 gpointer         user_data)
{
	log_message(LOG_INFO, "Acquired the name %s on the session bus\n", name);
}

/* run if bus name or connection are lost */
static void
on_name_lost(GDBusConnection *connection,
	     const gchar     *name,
	     gpointer         user_data)
{
	log_message(LOG_INFO, "Lost the name %s on the session bus\n", name);
	global_connection = connection;
	g_hash_table_foreach_remove(objects, unregister_object, NULL);
	objects = NULL;
	global_connection = NULL;
}

static gchar*
read_file(gchar* filepath)
{
	FILE * f;
	long length;
	gchar *ret = NULL;

	f = fopen(filepath, "rb");
	if (f) {
		fseek(f, 0, SEEK_END);
		length = ftell(f);
		fseek(f, 0, SEEK_SET);
		ret = MALLOC(length + 1);
		if (ret) {
			fread(ret, 1, length, f);
			ret[length] = '\0';
		}
		fclose(f);
	}
	return ret;
}

static void *
dbus_main(__attribute__ ((unused)) void *unused)
{
	gchar *introspection_xml;
	guint owner_id;

	objects = g_hash_table_new(g_str_hash, g_str_equal);

	/* DBus service org.keepalived.Vrrp1 exposes two interfaces, Vrrp and Instance.
	 * Vrrp is implemented by a single VRRP object for general purposes, such as printing
	 * data or signaling that the VRRP process has been stopped.
	 * Instance is implemented by an Instance object for every VRRP Instance in vrrp_data.
	 * It exposes instance specific methods and properties.
	 */
#ifdef DBUS_NEED_G_TYPE_INIT
	g_type_init();
#endif
	GError *error;

	/* read service interface data from xml files */
	introspection_xml = read_file(DBUS_VRRP_INTERFACE_FILE_PATH);
	if (!introspection_xml) {
		log_message(LOG_INFO, "Unable to read Dbus file %s", DBUS_VRRP_INTERFACE_FILE_PATH);
		return NULL;
	}
	vrrp_introspection_data = g_dbus_node_info_new_for_xml(introspection_xml, &error);
	FREE(introspection_xml);

	introspection_xml = read_file(DBUS_VRRP_INSTANCE_INTERFACE_FILE_PATH);
	if (!introspection_xml) {
		log_message(LOG_INFO, "Unable to read Dbus file %s", DBUS_VRRP_INSTANCE_INTERFACE_FILE_PATH);
		return NULL;
	}
	vrrp_instance_introspection_data = g_dbus_node_info_new_for_xml(introspection_xml, &error);
	FREE(introspection_xml);

	owner_id = g_bus_own_name(G_BUS_TYPE_SYSTEM,
				  DBUS_SERVICE_NAME,
				  G_BUS_NAME_OWNER_FLAGS_NONE,
				  on_bus_acquired,
				  on_name_acquired,
				  on_name_lost,
				  NULL,  /* user_data */
				  NULL); /* user_data_free_func */

	loop = g_main_loop_new(NULL, FALSE);
	g_main_loop_run(loop);

	/* cleanup after loop terminates */
	g_bus_unown_name(owner_id);
	global_connection = NULL;
	pthread_exit(0);
}

/* The following functions are run in the context of the main vrrp thread */

/* send signal VrrpStatusChange
 * containing the new state of vrrp */
void
dbus_send_state_signal(vrrp_t *vrrp)
{
	GError *local_error;
	/* the interface will go through the initial state changes before
	 * the main loop can be started and global_connection initialised */
	if (global_connection == NULL) {
		log_message(LOG_INFO, "Not connected to the %s bus", DBUS_SERVICE_NAME);
		return;
	}

	gchar *object_path = dbus_object_create_path_instance(IF_NAME(IF_BASE_IFP(vrrp->ifp)),
					g_strdup_printf("%d",vrrp->vrid));

	GVariant *args = g_variant_new("(u)", vrrp->state);
	g_dbus_connection_emit_signal(global_connection, NULL, object_path,
				      DBUS_VRRP_INSTANCE_INTERFACE,
				      "VrrpStatusChange", args, &local_error);

	g_free(object_path);
}

static void
return_dbus_msg(dbus_queue_ent_t *ent)
{
	pthread_mutex_lock(&out_queue_lock);
	list_add(dbus_out_queue, ent);
	pthread_mutex_unlock(&out_queue_lock);

	write(dbus_out_pipe[1], ent, 1);
}

static dbus_queue_ent_t *
get_queue_ent(void)
{
	dbus_queue_ent_t *ent = NULL;
	element e;

	pthread_mutex_lock(&in_queue_lock);
	if (!LIST_ISEMPTY(dbus_in_queue)) {
		e = LIST_HEAD(dbus_in_queue);
		free_list_element(dbus_in_queue, e);
		ent = e->data;
	}
	pthread_mutex_unlock(&in_queue_lock);

	return ent;
}

static int
handle_dbus_msg(thread_t *thread)
{
	dbus_queue_ent_t *ent;
	char recv_buf;
	list l;
	element e;
	vrrp_t *vrrp;
	char standardized_name[sizeof ((vrrp_t*)NULL)->ifp->ifname];

	read(dbus_in_pipe[0], &recv_buf, 1);

	if ((ent = get_queue_ent()) != NULL) {
		ent->reply = DBUS_SUCCESS;

		if (ent->action == DBUS_PRINT_DATA) {
			log_message(LOG_INFO, "Printing VRRP data on DBus request");
			vrrp_print_data();

		}
		else if (ent->action == DBUS_PRINT_STATS) {
			log_message(LOG_INFO, "Printing VRRP stats on DBus request");
			vrrp_print_stats();
		}
		else if (ent->action == DBUS_CREATE_INSTANCE) {
			gchar *name;
			g_variant_get(ent->args, "(s)", &name);

			if (g_hash_table_lookup(objects, name)){
				log_message(LOG_INFO, "An object for instance %s already exists", name);
				ent->reply = DBUS_OBJECT_ALREADY_EXISTS;	
			} else {
				gchar *path = dbus_object_create_path_instance(ent->str,
								g_strdup_printf("%d", ent->val));

				guint instance = g_dbus_connection_register_object(global_connection, path,
									vrrp_instance_introspection_data->interfaces[0],
									&interface_vtable, NULL, NULL, NULL);

				if (instance != 0){
					g_hash_table_insert(objects, name, GUINT_TO_POINTER(instance));
					log_message(LOG_INFO, "Added DBus object for instance %s on path %s", name, path);
				}
				g_free(path);
			}
		}
		else if (ent->action == DBUS_DESTROY_INSTANCE) {
			gchar *key = ent->str;
			guint value = g_hash_table_lookup(objects, key);
			if (value){
				unregister_object(key, value, NULL);
				g_hash_table_remove(objects, ent->str);
				log_message(LOG_INFO, "Deleted DBus object for instance %s", ent->str);
			} else {
				log_message(LOG_INFO, "DBus object not found for instance %s", ent->str);
			}

		}
		else if (ent->action == DBUS_SEND_GARP) {
			ent->reply = DBUS_INTERFACE_NOT_FOUND;
			l = vrrp_data->vrrp;
			if (!LIST_ISEMPTY(l)) {
				for (e = LIST_HEAD(l); e; ELEMENT_NEXT(e)) {
					vrrp = ELEMENT_DATA(e);
					if (!strcmp(vrrp->iname, ent->str)) {
						log_message(LOG_INFO, "Sending garps on %s on DBus request", vrrp->iname);
						vrrp_send_link_update(vrrp, 1);
						ent->reply = DBUS_SUCCESS;
						break;
					}
				}
			}
		}
		else if (ent->action == DBUS_GET_NAME ||
			 ent->action == DBUS_GET_STATUS) {
			/* we look for the vrrp instance object that corresponds to our interface and group */
			ent->reply = DBUS_INTERFACE_NOT_FOUND;

			if (!LIST_ISEMPTY(vrrp_data->vrrp)) {
				for (e = LIST_HEAD(vrrp_data->vrrp); e; ELEMENT_NEXT(e)) {
					vrrp_t * vrrp = ELEMENT_DATA(e);

					/* The only valid characters in a path a A-Z, a-z, 0-9, _ */
					set_valid_path(standardized_name, IF_NAME(IF_BASE_IFP(vrrp->ifp)));

					if (!strcmp(ent->str, standardized_name) && ent->val == vrrp->vrid) {
						/* the property_name argument is the property we want to Get */
						if (ent->action == DBUS_GET_NAME)
							strcpy(ent->str, vrrp->iname);
						else if (ent->action == DBUS_GET_STATUS)
							ent->val = vrrp->state;
						else {
							/* How did we get here? */
							ent->val = 0;
							ent->str[0] = '\0';
						}
						ent->reply = DBUS_SUCCESS;
						break;
					}
				}
			}
		}
		return_dbus_msg(ent);
	}

	thread_add_read(master, handle_dbus_msg, NULL, dbus_in_pipe[0], TIMER_NEVER);

	return 0;
}

bool
dbus_start(void)
{
	pthread_t dbus_thread;

	dbus_in_queue = alloc_list(NULL, NULL);
	dbus_out_queue = alloc_list(NULL, NULL);

#ifdef HAVE_PIPE2
	if (pipe2(dbus_in_pipe, O_CLOEXEC)) {
		log_message(LOG_INFO, "Unable to create inbound dbus pipe - disabling DBus");
		return false;
	}
	if (pipe2(dbus_out_pipe, O_CLOEXEC)) {
		log_message(LOG_INFO, "Unable to create outbound dbus pipe - disabling DBus");
		return false;
	}
#else
	if (pipe(dbus_in_pipe)) {
		log_message(LOG_INFO, "Unable to create inbound dbus pipe - disabling DBus");
		return false;
	}
	if (pipe(dbus_out_pipe)) {
		log_message(LOG_INFO, "Unable to create outbound dbus pipe - disabling DBus");
		return false;
	}
	fcntl (dbus_in_pipe[0], F_SETFD, FD_CLOEXEC | fcntl(dbus_in_pipe[0], F_GETFD));
	fcntl (dbus_in_pipe[1], F_SETFD, FD_CLOEXEC | fcntl(dbus_in_pipe[1], F_GETFD));
	fcntl (dbus_out_pipe[0], F_SETFD, FD_CLOEXEC | fcntl(dbus_out_pipe[0], F_GETFD));
	fcntl (dbus_out_pipe[1], F_SETFD, FD_CLOEXEC | fcntl(dbus_out_pipe[1], F_GETFD));
#endif

	/* We don't want the main thread to block when using the pipes */
	fcntl(dbus_in_pipe[0], F_SETFL, O_NONBLOCK | fcntl(dbus_in_pipe[0], F_GETFL));
	fcntl(dbus_out_pipe[1], F_SETFL, O_NONBLOCK | fcntl(dbus_out_pipe[1], F_GETFL));

	thread_add_read(master, handle_dbus_msg, NULL, dbus_in_pipe[0], TIMER_NEVER);

	/* Now create the dbus thread */
	pthread_create(&dbus_thread, NULL, &dbus_main, NULL);

	return true;
}

void
dbus_stop(void)
{
	GError *local_error;

	pthread_mutex_lock(&in_queue_lock);
	free_list(&dbus_in_queue);
	dbus_in_queue = NULL;
	pthread_mutex_unlock(&in_queue_lock);

	pthread_mutex_lock(&out_queue_lock);
	free_list(&dbus_out_queue);
	dbus_out_queue = NULL;
	pthread_mutex_unlock(&out_queue_lock);

	if (global_connection != NULL)
		g_dbus_connection_emit_signal(global_connection, NULL, dbus_object_create_path_vrrp(),
					      DBUS_VRRP_INTERFACE, "VrrpStopped", NULL, &local_error);
	g_main_loop_quit(loop);

	g_dbus_node_info_unref(vrrp_introspection_data);
	g_dbus_node_info_unref(vrrp_instance_introspection_data);
}
