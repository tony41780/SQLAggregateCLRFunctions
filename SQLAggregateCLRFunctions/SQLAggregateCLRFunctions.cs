using System;
using System.Collections.Generic;
using System.Text;

using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

[Serializable]
[SqlUserDefinedAggregate(
    Format.UserDefined,
    IsInvariantToDuplicates = true,
    IsInvariantToNulls = true,
    IsInvariantToOrder = false,
    IsNullIfEmpty = true,
    MaxByteSize = 200,
    Name = "FirstValue"
    )]
public struct FirstValue : IBinarySerialize
{
    private string rtnString;
    public void Init()
    {
        rtnString = "";
    }

    public void Accumulate(SqlString value) {
        if (value.IsNull || rtnString != "")
        {
            return;
        }
        rtnString = value.ToString();
    }
    public void Merge(FirstValue Group)
    {
        rtnString = Group.rtnString;
    }
    public SqlString Terminate()
    {
        return new SqlString(rtnString);
    }
    public void Read(System.IO.BinaryReader SerializationReader)
    {
        this.rtnString = SerializationReader.ReadString();
    }

    public void Write(System.IO.BinaryWriter SerializationWriter)
    {
        SerializationWriter.Write(this.rtnString);        
    }
}

[Serializable]
[SqlUserDefinedAggregate(
    Format.UserDefined,
    IsInvariantToDuplicates = true,
    IsInvariantToNulls = true,
    IsInvariantToOrder = false,
    IsNullIfEmpty = true,
    MaxByteSize = 200,
    Name = "FirstValueSort"
    )]
public struct FirstValueSort : IBinarySerialize
{
    public string rtnString;    
    private string prmSortString;    
    public void Init()
    {
        rtnString = "";
        prmSortString = "";        
    }

    public void Accumulate(SqlString value, SqlString sortString, SqlInt32 sortDescending)
    {
        if (value.IsNull)
        {
            return;
        }
        if ((sortDescending == 1
                && (String.Compare(prmSortString, sortString.ToString() ?? "") >= 0 || prmSortString=="")) 
            || (sortDescending == 0
                && String.Compare(prmSortString, sortString.ToString() ?? "") == -1))
        {
            rtnString = value.ToString();
            prmSortString = sortString.ToString();        
        }               
    }
    public void Merge(FirstValueSort Group)
    {
        rtnString = Group.rtnString;
    }
    public SqlString Terminate()
    {
        return new SqlString(rtnString);
    }
    public void Read(System.IO.BinaryReader SerializationReader)
    {
        this.rtnString = SerializationReader.ReadString();
        this.prmSortString = SerializationReader.ReadString();        
    }

    public void Write(System.IO.BinaryWriter SerializationWriter)
    {
        SerializationWriter.Write(this.rtnString);
        SerializationWriter.Write(this.prmSortString);
    }
}