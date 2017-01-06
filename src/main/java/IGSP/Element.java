package IGSP;

public class Element
{
	private String item;

	public Element()
	{
		item = new String();
	}
	public Element(String item)
	{
		this.item = new String();
		this.item = item;
	}
	public Element clone()
	{
		Element clone = new Element();
		clone.item = this.item;
		return clone;
	}
	public String getItem()
	{
		return item;
	}
	@Override
	public String toString()
	{
		return item;
	}
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((item == null) ? 0 : item.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Element other = (Element) obj;
		if (item == null)
		{
			if (other.item != null)
				return false;
		} else if (!item.equals(other.item))
			return false;
		return true;
	}
}
