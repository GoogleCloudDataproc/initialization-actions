/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.testing;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;

import org.junit.Assert;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides exception checking related helpers.
 */
public class ExceptionUtil {

  // Method name to pass when checking for exceptions in a constructor.
  public static final String CONSTRUCTOR = "<init>";

  /**
   * Verifies that the given method throws the given exception.
   *
   * @param expectedException type of exception expected to be thrown.
   * @param instance the object whose method is to be called. In case of static methods or
   *        constructors, this parameter must be class type (instead of object instance).
   * @param methodName name of the method to call. Empty string implies constructor.
   * @param params parameter values to be passed to the method.
   */
  public static void checkThrows(Class<? extends Exception> expectedException,
      Object instance, String methodName, Object... params) {
    checkThrowsWithMessage(expectedException, null, instance, methodName, params);
  }

  /**
   * Verifies that the given method throws the given exception and that the exception's
   * message contains the provided {@code expectedSubMessage}.
   *
   * @param expectedException type of exception expected to be thrown.
   * @param expectedSubMessage a String expected to be contained within the exception's
   *     getMessage(), or null to not care about the exception's getMessage().
   * @param instance the object whose method is to be called. In case of static methods or
   *     constructors, this parameter must be class type (instead of object instance).
   * @param methodName name of the method to call. Empty string implies constructor.
   * @param params parameter values to be passed to the method.
   */
  public static void checkThrowsWithMessage(Class<? extends Exception> expectedException,
      String expectedSubMessage, Object instance, String methodName, Object... params) {
    Preconditions.checkArgument(expectedException != null, "expectedException must not be null");
    Preconditions.checkArgument(instance != null, "instance must not be null");
    Preconditions.checkArgument(methodName != null, "methodName must not be null");

    // For static methods, instance is passed as instance type.
    Class<?> instanceType;
    if (instance instanceof Class<?>) {
      instanceType = (Class<?>) instance;
    } else {
      instanceType = instance.getClass();
    }

    // Call the given method using reflection and verify that the expected exception is thrown.
    boolean findConstructor = methodName.equals(CONSTRUCTOR);
    String msg = "Expected exception: " + expectedException.getName();
    if (expectedSubMessage != null) {
      msg = msg + String.format(" with message containing the substring: '%s'", expectedSubMessage);
    }
    Object result = findMethod(instanceType, methodName, params);
    if (result == null) {
      Assert.fail((findConstructor ? "Constructor not found" : "Method not found: " + methodName)
          + " for params: " + Lists.newArrayList(params));
    }
    try {
      if (findConstructor) {
        ((Constructor) result).newInstance(params);
      } else {
        ((Method) result).invoke(instance, params);
      }
      Assert.fail(msg);
    } catch (IllegalAccessException e) {
      Assert.fail("Method not accessible: " + methodName);
    } catch (InstantiationException | InvocationTargetException e) {
      Throwable methodException = e.getCause();
      boolean matchesClass = expectedException.isAssignableFrom(methodException.getClass());
      boolean matchesSubMessage = (expectedSubMessage == null
          || String.valueOf(methodException.getMessage()).contains(expectedSubMessage));
      if (!matchesClass || !matchesSubMessage) {
        Assert.fail(msg + ", actual exception: " + methodException.toString());
      }
    }
  }

  /**
   * Finds method with the given name in given the class. If method name is empty, a constructor
   * is searched for instead.
   *
   * Method parameter types must be deduced from the supplied parameter values.
   * In the simple case, this can be done simply by calling paramValue.getClass().
   * However, we need to address 2 special cases:
   * -- null values must be passed as the intended type instead (cannot call getClass() on it).
   *    This can make it impossible to use this method to verify exceptions thrown from a method
   *    that really accepts a class parameter. However, we do not have any such methods to test.
   *    Even if we did, we could use the simple try~catch method to test their exceptions.
   * -- method parameter type could be an interface while the passed values could be a type
   *    that supports many interfaces. We must exhaustively try each possible interface type
   *    to see which fits the method signature.
   */
  private static Object findMethod(Class<?> instanceType, String methodName, Object[] params) {
    // Extract parameter types from the parameter values passed.
    // A null value is represented by passing its class type instead.
    Class<?>[] paramTypes = new Class<?>[params.length];
    return findMethod(instanceType, methodName, params, paramTypes, 0);
  }

  /**
   * Helper to recursively get superclasses, unwrapped primitives, and interfaces of a given param
   * class, dumping them all into {@code paramTypes}.
   */
  private static void recursivelyAddClassesAndInterfaces(
      List<Class<?>> paramTypes, Class<?> paramClass) {
    // Add the param's class itself.
    paramTypes.add(paramClass);

    // Add all direct interfaces implemented.
    for (Class<?> iface : paramClass.getInterfaces()) {
      paramTypes.add(iface);
    }

    // If the param is one of the special wrapper types (e.g. Integer, Boolean, etc.) then we
    // must also try searching with the actual primitive type (int.class != Integer.class);
    // we can't simply replace with the actual primitive type, since the method might actually
    // be implemented to take the wrapper type explicitly.
    if (Primitives.isWrapperType(paramClass)) {
      paramTypes.add(Primitives.unwrap(paramClass));
    }

    // Recurse for superclasses.
    Class<?> superClass = paramClass.getSuperclass();
    if (superClass != null) {
      recursivelyAddClassesAndInterfaces(paramTypes, superClass);
    }
  }

  /**
   * Helper to find method as described earlier using exhaustive search.
   */
  private static Object findMethod(Class<?> instanceType, String methodName, Object[] params,
      Class<?>[] paramTypes, int index) {

    if (index >= params.length) {
      return getMethod(instanceType, methodName, paramTypes);
    }

    Preconditions.checkArgument(params[index] != null,
        String.format("null value must be represented as a class type at index %d, params: %s",
            index, Lists.newArrayList(params)));

    // If we end up returning null and the caller must continue recursing, we must make sure we
    // restore the 'Object[] params' to its previous state.
    Object oldParam = null;

    // First, generate a list of types to try at the given index.
    List<Class<?>> paramAtIndexTypes = new ArrayList<>();
    if (params[index] instanceof Class<?>) {
      // Handle special case #1.
      // That is, if the supplied parameter is already a class type then use that type and
      // change the value back to null.
      if (((Class<?>) params[index]).isPrimitive()) {
        Assert.fail(String.format("Cannot attempt to pass null for param of type '%s' at index %d",
            params[index], index));
      }
      paramAtIndexTypes.add((Class<?>) params[index]);
      oldParam = params[index];
      params[index] = null;
    } else {
      // Handle special case #2. Try self, all interfaces, and all unwrapped primitives recursively
      // to include the same for all superclasses.
      Class<?> paramClass = params[index].getClass();
      recursivelyAddClassesAndInterfaces(paramAtIndexTypes, paramClass);
    }

    // Try each potential class type to see if we have a match.
    for (Class<?> type : paramAtIndexTypes) {
      paramTypes[index] = type;
      Object method = findMethod(instanceType, methodName, params, paramTypes, index + 1);
      if (method != null) {
        return method;
      }
    }

    if (oldParam != null) {
      params[index] = oldParam;
    }

    // No match found.
    return null;
  }

  /**
   * Gets method with the given name or constructor if method name is empty; else returns null
   * if a matching method or constructor is not found.
   */
  private static Object getMethod(Class<?> type, String methodName, Class<?>[] paramTypes) {
    try {
      if (methodName.equals(CONSTRUCTOR)) {
        return type.getConstructor(paramTypes);
      } else {
        return type.getMethod(methodName, paramTypes);
      }
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
